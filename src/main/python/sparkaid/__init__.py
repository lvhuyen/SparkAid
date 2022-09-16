from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType, BooleanType, DataType, IntegerType, LongType, NullType, StringType, StructField, StructType)
from pyspark.sql import DataFrame
from typing import Callable, List, Optional, Tuple

import json
import logging
import re


def __normalise_fieldname__(raw: str):
    return re.sub('[^A-Za-z0-9_]+', '_', raw.strip())


def __get_fields_info__(dtype: DataType, name: str = ""):
    ret = []
    if isinstance(dtype, StructType):
        for field in dtype.fields:
            for child in __get_fields_info__(field.dataType, field.name):
                wrapped_child = ["{prefix}{suffix}".format(
                    prefix=("" if name == "" else "`{}`.".format(name)), suffix=child[0])] + child[1:]
                ret.append(wrapped_child)
    elif isinstance(dtype, ArrayType) and (
            isinstance(dtype.elementType, ArrayType) or isinstance(dtype.elementType, StructType)):
        for child in __get_fields_info__(dtype.elementType):
            wrapped_child = ["`{}`".format(name)] + child
            ret.append(wrapped_child)
    else:
        return [["`{}`".format(name)]]
    return ret


def snake_case(raw: str) -> str:
    """
    Convert the input string to snake_case format.
    All Special characters are converted to _
    Consecutive uppercase or digit characters are converted to lowercase
    An uppercase or digit, when followed by a lowercase and not following an _, is padded by an _
    :param raw:
    :return:
    """
    ret = ""
    last_char = -1
    c: str
    for c in reversed(raw):
        if not c.isalnum():
            ret = "_" + ret
            last_char = -1
        elif c.islower():
            ret = (c + "_" if last_char > 0 else c) + ret
            last_char = 0
        elif last_char == 9:
            ret = c.lower() + "_" + ret
            last_char = -1
        else:
            ret = c.lower() + ret
            last_char = 9 if last_char == 0 else 5
    return ret


def normalise_fields_names(df: DataFrame, fieldname_normaliser=__normalise_fieldname__):
    """
    Recursively apply fieldname_normaliser() on all fieldnames in the provided DataFrame
    :param df:
    :param fieldname_normaliser:
    :return:
    """
    def __rename_nested_field__(in_field: DataType):
        if isinstance(in_field, ArrayType):
            dtype = ArrayType(__rename_nested_field__(in_field.elementType),
                              in_field.containsNull)
        elif isinstance(in_field, StructType):
            dtype = StructType()
            for field in in_field.fields:
                dtype.add(fieldname_normaliser(field.name),
                          data_type=__rename_nested_field__(field.dataType),
                          nullable=field.nullable,
                          metadata=field.metadata,
                          )
        else:
            dtype = in_field
        return dtype

    return df.select([
        F.col("`{}`".format(field.name)).cast(__rename_nested_field__(field.dataType))
            .alias(fieldname_normaliser(field.name)) for field in df.schema.fields
    ])


def flatten(df: DataFrame,
            fieldname_normaliser: Callable = __normalise_fieldname__,
            nested_struct_separator: str = "__",
            arrays_to_unpack: Optional[List[str]] = None) -> DataFrame:
    """
    Flatten a nested DataFrame:
        - Struct fields will be un-packed
        - Array fields will stay as Array, but their nested structs will be flattened, recursively
    :param df:
    :param nested_struct_separator:
    :param fieldname_normaliser:
    :return:
    """
    if not arrays_to_unpack:
        def _unpack_struct(struct: StructType, parent_h: str, parent_f) -> List[Tuple[str, str]]:
            """ Return (hierarchical name, flattened name) for each leaf field, prefixed by (parent_h, parent_f) """
            ret = []
            for f in struct.fields:
                h_name = ".".join(filter(None, [parent_h, f"`{f.name}`"]))
                f_name = nested_struct_separator.join(filter(None, [parent_f, fieldname_normaliser(f.name)]))
                if isinstance(f.dataType, StructType):
                    ret += _unpack_struct(f.dataType, parent_h=h_name, parent_f=f_name)
                else:
                    ret.append((h_name, f_name))
            return ret
        return df.select(*[F.col(h).alias(f) for h, f in _unpack_struct(df.schema, "", "")])

    elif arrays_to_unpack == ["*"]:
        cols = []
        for child in __get_fields_info__(df.schema):
            if len(child) > 2:
                ex = "x.{}".format(child[-1])
                for seg in child[-2:0:-1]:
                    if seg != '``':
                        ex = "transform(x.{outer}, x -> {inner})".format(outer=seg, inner=ex)
                ex = "transform({outer}, x -> {inner})".format(outer=child[0], inner=ex)
            else:
                ex = ".".join(child)
            cols.append(F.expr(ex).alias(
                fieldname_normaliser(nested_struct_separator.join(child).replace('`', ''))))
        return df.select(cols)

    else:
        raise Exception("Feature not supported. "
                        "The support for specifying the list of arrays to unpack will be added in the next release.")


def json_schema_to_spark_schema(raw: dict) -> StructType:
    """
    Convert a JSON Schema (https://json-schema.org/) to a Spark Schema
    :param raw:
    :return:
    """
    def __convert_struct_field(name: str, raw_type_def: dict) -> StructField:
        logging.debug(f"converting field '{name}' of type {raw_type_def}")

        nullable = False
        # Full-form compound type
        compound_type_def = raw_type_def.get("anyOf", raw_type_def.get("oneOf"))
        if compound_type_def and isinstance(compound_type_def, list):
            spark_type = NullType()
            for cur_type_def in compound_type_def:
                cur_type = __convert_struct_field("__", cur_type_def).dataType
                if cur_type == NullType():
                    nullable = True
                elif spark_type == NullType():
                    spark_type = cur_type
                else:
                    raise Exception(f"Compound type {json.dumps(compound_type_def)} is not supported.")

        else:
            # Compact form Compound type
            raw_type = raw_type_def["type"]
            if isinstance(raw_type, list):
                if "null" in raw_type:
                    nullable = True
                    raw_type.remove("null")
                if len(raw_type) > 1:
                    raise Exception(f"Compound type {raw_type} in {raw_type_def} is not supported.")
                elif len(raw_type) == 1:
                    raw_type = raw_type[0]
                else:
                    raw_type = "null"

            # Non-compound type
            if raw_type == "object":
                if "properties" not in raw_type_def.keys():
                    raise Exception(f"Invalid schema: object type '{name}' has no 'properties'.")
                spark_type = json_schema_to_spark_schema(raw_type_def)
            elif raw_type == "array":
                if "items" not in raw_type_def.keys():
                    raise Exception(f"Invalid schema: array type '{name} has no 'items'.")
                element_field = __convert_struct_field("dummy", raw_type_def["items"])
                spark_type = ArrayType(elementType=element_field.dataType, containsNull=element_field.nullable)
            else:
                spark_type = {
                    "boolean": BooleanType(),
                    "number": LongType(),
                    "integer": IntegerType(),
                    "string": StringType(),
                    "null": NullType()
                }.get(raw_type)
                if not spark_type:
                    raise Exception(f"Type '{raw_type}' in {raw_type_def} is not supported.")

        return StructField(name=name, dataType=spark_type, nullable=nullable)

    for k in raw.keys():
        if k not in ["type", "properties"]:
            logging.warning(f"schema field {k} in {raw} is not supported. Expecting either 'type' or 'properties'")

    return StructType(
        fields=[__convert_struct_field(n, t) for n, t in raw["properties"].items()]
    )
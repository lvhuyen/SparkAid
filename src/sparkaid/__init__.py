from pyspark.sql import functions as F
from pyspark.sql.types import DataType, StructType, ArrayType
from pyspark.sql import DataFrame
import re


def __rename_nested_field__(in_field: DataType, fieldname_normaliser):
	if isinstance(in_field, ArrayType):
		dtype = ArrayType(__rename_nested_field__(in_field.elementType, fieldname_normaliser), in_field.containsNull)
	elif isinstance(in_field, StructType):
		dtype = StructType()
		for field in in_field.fields:
			dtype.add(fieldname_normaliser(field.name), __rename_nested_field__(field.dataType, fieldname_normaliser))
	else:
		dtype = in_field
	return dtype


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


def normalise_fields_names(df: DataFrame, fieldname_normaliser=__normalise_fieldname__):
	return df.select([
		F.col("`{}`".format(field.name)).cast(__rename_nested_field__(field.dataType, fieldname_normaliser))
			.alias(fieldname_normaliser(field.name)) for field in df.schema.fields
	])


def flatten(df: DataFrame, fieldname_normaliser=__normalise_fieldname__):
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
		cols.append(F.expr(ex).alias(fieldname_normaliser("_".join(child).replace('`', ''))))
	return df.select(cols)
import pytest
import re
from pyspark.sql import SparkSession, DataFrame
from typing import Callable

from sparkaid import snake_case, normalise_fields_names, flatten as flatten


@pytest.mark.parametrize("raw,expected", [
    ("SheIsBeautiful", "she_is_beautiful"),
    ("love.me", "love_me"),
    ("LOVE.Me", "love_me"),
    ("Elle.M'aime.beaucoup", "elle_m_aime_beaucoup"),
    ("Elle.M'.aime.beaucoup", "elle_m__aime_beaucoup"),
    ("ABC", "abc"),
    ("ABc", "a_bc"),
    ("aBC", "a_bc"),
    ("The1st", "the_1st"),
    ("The.2nd", "the_2nd"),
    ("SIXTY9", "sixty9"),
    ("Hi5", "hi_5"),
])
def test_snake_case(raw, expected):
    assert snake_case(raw) == expected


builder = SparkSession.builder
spark = builder.master("local[*]").getOrCreate()
raw_json = re.sub("\\s+", "", """{
    "PrimaryStr": "a",
    "Secondary-Struct": {
        "Field:Int": 3,
        "Field:List": [
            {
                "BoolElement": true,
                "FloatElement": 1.2
            }
        ]
    }
}""")
raw_df = spark.read.json(spark.sparkContext.parallelize([raw_json]))

normalized_json = re.sub("\\s+", "", """{
    "primary_str": "a",
    "secondary_struct": {
        "field_int": 3,
        "field_list": [
            {
                "bool_element": true,
                "float_element": 1.2
            }
        ]
    }
}""")
normalized_df = spark.read.json(spark.sparkContext.parallelize([normalized_json]))

flattened_json = re.sub("\\s+", "", """{
    "primary_str": "a",
    "secondary_struct___field_int": 3,
    "secondary_struct___field_list": [
        {
            "BoolElement": true,
            "FloatElement": 1.2
        }
    ]
}""")
flattened_df = spark.read.json(spark.sparkContext.parallelize([flattened_json]))


@pytest.mark.parametrize("raw,normalizer,expected", [
    (raw_df, snake_case, normalized_df),
    (raw_df, lambda x: x, raw_df),
])
def test_snake_case_schema(raw: DataFrame, expected: DataFrame, normalizer: Callable):
    def _schema_diff(df_1: DataFrame, df_2: DataFrame):
        s1 = spark.createDataFrame(df_1.dtypes, ["d1_name", "d1_type"])
        s2 = spark.createDataFrame(df_2.dtypes, ["d2_name", "d2_type"])
        difference = (
            s1.join(s2, s1.d1_name == s2.d2_name, how="outer")
                .where(s1.d1_type.isNull() | s2.d2_type.isNull())
                .select(s1.d1_name, s1.d1_type, s2.d2_name, s2.d2_type)
                .fillna("")
        )
        return difference

    output_df = normalise_fields_names(raw, fieldname_normaliser=normalizer)
    diff = _schema_diff(output_df, expected)
    assert not diff.collect()


@pytest.mark.parametrize("raw,expected", [
    (raw_df, flattened_df),
])
def test_flatten_dataframe(raw: DataFrame, expected: DataFrame):
    output_df = flatten(raw, fieldname_normaliser=snake_case,
                        nested_struct_separator="___")
    output_df.show()
    expected.show()
    assert output_df.subtract(expected).count() == 0
    assert expected.subtract(output_df).count() == 0

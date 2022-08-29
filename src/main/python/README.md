# Spark - Dataframe with complex schema

# Changelog
### Version 1.0.0:
* **_[Breaking changes]_** `flatten()` now stops the unpacking of nested data at ArrayType
  (i.e: any field with DataType = ArrayType will have its nested elements as-is).
  To have the same result as in the previous version - flatten all array fields, add the param `arrays_to_unpack = ["*"]`.
* Added `snake_case()`
* Added `json_schema_to_spark_schema()`
* Added support for providing the param `nested_struct_separator` to `flatten()`.
  Example: When provided with the value "___", the raw schema `{"parent": {"child": "some_value"}}` will be unpacked to `{"parrent___child": "some_value"}`

# Problem description
A Spark DataFrame can have a simple schema, where every single column is of a simple datatype like `IntegerType, BooleanType, StringType`,...
However, a column can be of one of the complex types: `ArrayType`, `MapType`, or `StructType`. The schema itself is, actually, an instance of the type `StructType`. So, when a schema has column(s) with DataType is StructType, we have a nested schema.

Working with nested schema is not always easy. Some notable problems are:
* Complex SQL queries
* Difficult to rename/cast datatype of nested columns
* Unnecessary high IO when reading only some nested columns from Parquet files (_https://issues.apache.org/jira/browse/SPARK-17636_)

The page *https://docs.databricks.com/delta/data-transformation/complex-types.html* provides a lot of useful tips on dealing with dataframes having a complex schema.<br>

This page will provide some further tips/utils to work on dataframes with more complex schema:
* Renaming or casting/parsing nested columns
* Flattening

_Please note the term **Flattening** in this post **only** means getting rid of StructType column(s) in our DataFrame. This does not include the act of eliminating ArrayType and MapType in the schema, which is usually called **Exploding** in Spark documents. Also, Spark 2.4 introduced the function flatten [https://spark.apache.org/docs/2.4.0/api/java/org/apache/spark/sql/functions.html#flatten-org.apache.spark.sql.Column], which is used on a nested array (an array with 2 layers, an array of arrays of DataType) to make it flat (an array with 1 layer)._

# Solutions
## Renaming nested columns
Renaming a column at root level is simple: use the function `withColumnRenamed`.
However, with a nested column, that function does not give any error, but also does not make any effect:

	df_struct = spark.createDataFrame([Row(structA=Row(field1=10, field2=1.5), structB=Row(field3="one",field4=False))])
    df_struct.printSchema()
	
	root
     |-- structA: struct (nullable = true)
     |    |-- field1: long (nullable = true)
     |    |-- field2: double (nullable = true)
     |-- structB: struct (nullable = true)
     |    |-- field3: string (nullable = true)
     |    |-- field4: boolean (nullable = true)
    
    df_struct.withColumnRenamed("structA.field1", "structA.newField1") \
        .withColumnRenamed("structB", "newStructB") \
        .printSchema()

	root
     |-- structA: struct (nullable = true)
     |    |-- field1: long (nullable = true)
     |    |-- field2: double (nullable = true)
     |-- newStructB: struct (nullable = true)
     |    |-- field3: string (nullable = true)
     |    |-- field4: boolean (nullable = true)

To change the names of nested columns, there are some options:
1. By building a new struct column on the flight with the `struct()` function:

		from pyspark.sql.functions import struct, col
		df_renamed = df_struct.withColumn("structA", struct(col("structA.field1").alias("newField1"),
		                                                    col("structA.field2")))

2. By creating a new *schema* (a `StructType()` object) and use type casting on the original struct column:

		from pyspark.sql.types import *		
		newStructASchema = StructType([
	                            StructField("newField1", LongType()),
	                            StructField("field2", DoubleType())
	                        ])
	    df_renamed = df_struct.withColumn("structA", col("structA").cast(newStructASchema)).printSchema()
	    
Both options yield the same schema

	    root
	     |-- structA: struct (nullable = true)
	     |    |-- newField1: long (nullable = true)
	     |    |-- field2: double (nullable = true)
	     |-- structB: struct (nullable = true)
	     |    |-- field3: string (nullable = true)
	     |    |-- field4: boolean (nullable = true)

The 2nd option is more convenient when building a recursive function to recreate the multi-layer nested schema with new columns names.

---

## Flattening
### StructType

Sample DataFrame:

    from pyspark.sql import Row
    from pyspark.sql.functions import col

    df_struct = spark.createDataFrame([Row(structA=Row(field1=10, field2=1.5),
                                           structB=Row(field3="one",field4=False))])
	df_struct.printSchema()

	root
	 |-- structA: struct (nullable = true)
	 |    |-- field1: long (nullable = true)
	 |    |-- field2: double (nullable = true)
	 |-- structB: struct (nullable = true)
	 |    |-- field3: string (nullable = true)
	 |    |-- field4: boolean (nullable = true)

Spark allows selecting nested columns by using the dot `.` notation:

	df_struct.select("structA.*", "structB.field3").printSchema()
	
	root
     |-- field1: long (nullable = true)
     |-- field2: double (nullable = true)
     |-- field3: string (nullable = true)
Please note here that the current Spark implementation (2.4.3 or below) doesn't keep the outer layer fieldname (e.g: structA) in the output dataframe

### ArrayType
To select only some elements from an ArrayType column, either *`getItem()`* or using brackets (as selecting elements from a legacy array: `[]` in Python `()` in Scala)  would do the trick:

	df_array = spark.createDataFrame([Row(arrayA=[1,2,3,4,5],fieldB="foo")])
	df_array.select(col("arrayA").getItem(0).alias("element0"), col("arrayA")[4].alias("element5"), col("fieldB")).show()
	
	+--------+--------+------+
    |element0|element5|fieldB|
    +--------+--------+------+
    |       1|       5|   foo|
    +--------+--------+------+

### MapType
Elements from a MapType column can be selected the same way as in the case of ArrayType, but using the key instead of index number. The dot notation (`.`) could also be used instead of `getItem()` or brackets:

    df_map = spark.createDataFrame([Row(mapA={2: "TWO", 3: "THREE", 0: "ZERO"}, fieldB="foo")])
    df_map.select(col("mapA")[3].alias("element3"), col("mapA").getItem(2).alias("element2"), col("mapA.0").alias("element0"), col("mapA").getItem(1).alias("element1")).show()
	
	+--------+--------+--------+--------+
    |element3|element2|element0|element1|
    +--------+--------+--------+--------+
    |   THREE|     TWO|    ZERO|    null|
    +--------+--------+--------+--------+

### StructType nested in StructType
As Spark DataFrame.select() supports passing an array of columns to be selected, to fully unflatten a multi-layer nested dataframe, a recursive call would do the trick.

Here is a detailed discussion on StackOverFlow on how to do this:
https://stackoverflow.com/questions/37471346/automatically-and-elegantly-flatten-dataframe-in-spark-sql

### StructType nested in ArrayType
	df_nested = spark.createDataFrame([
	    Row(
	        arrayA=[
	            Row(childStructB=Row(field1=1, field2="foo")),
	            Row(childStructB=Row(field1=2, field2="bar"))
	        ]
	    )])
	df_nested.printSchema()
	
	root
     |-- arrayA: array (nullable = true)
     |    |-- element: struct (containsNull = true)
     |    |    |-- childStructB: struct (nullable = true)
     |    |    |    |-- field1: long (nullable = true)
     |    |    |    |-- field2: string (nullable = true)
     
    df_nested.show(1, False)
    
    +------------------------+
    |arrayA                  |
    +------------------------+
    |[[[1, foo]], [[2, bar]]]|
    +------------------------+
     
Selecting *field1* or *field2* can be done as with normal structs (not nested inside an array), by using that dot `.` annotation. The result would be of the type `ArrayType[ChildFieldType]`, which has been **_vertically sliced_** from the original array

	df_child = df_nested.select("arrayA.childStructB.field1", "arrayA.childStructB.field2")
	df_child.printSchema()
	
	root
     |-- field1: array (nullable = true)
     |    |-- element: long (containsNull = true)
     |-- field2: array (nullable = true)
     |    |-- element: string (containsNull = true)
     
    df_child.show()

    +------+----------+
    |field1|    field2|
    +------+----------+
    |[1, 2]|[foo, bar]|
    +------+----------+

### StructType nested in MapType
As each MapType column has two components, the keys and the values, selecting nested column inside a MapType column is not straight forward - we cannot just use that `.` to take the nested fields as that has already been used for denoting the key. 

    df_map_nested = spark.createDataFrame([Row(mapA={"2": Row(type_name="Arabic number", equivalent=2), "THREE": Row(type_name="English Text", equivalent=3)}, fieldB="foo")])
    df_map_nested.select(col("mapA.type_name"), col("mapA.THREE.type_name")).show()
    
    +---------+------------+
    |type_name|   type_name|
    +---------+------------+
    |     null|English Text|
    +---------+------------+
    
A solution for this is to use the builtin function `map_values()` which has been introduced since Spark 2.3. Note the type of the result column: ArrayType

    from pyspark.sql.functions import map_values
    result = df_map_nested.select(map_values("mapA")["type_name"], col("mapA.THREE.type_name"))
    result.show(2,False)
    result.printSchema()

    +-----------------------------+------------+
    |map_values(mapA).type_name   |type_name   |
    +-----------------------------+------------+
    |[Arabic number, English Text]|English Text|
    +-----------------------------+------------+
    
    root
     |-- map_values(mapA).type_name: array (nullable = true)
     |    |-- element: string (containsNull = true)
     |-- type_name: string (nullable = true)
     
	
## Hurdles
The above steps would work well for most of dataframes. The only dataframes that it fails (as of Spark 2.4.3 or lower) are the ones with a StructType nested inside MORE THAN ONE layers of ArrayType.
Like this one:

	df_nested_B = spark.createDataFrame([
        Row(
            arrayA=[[
                Row(childStructB=Row(field1=1, field2="foo")),
                Row(childStructB=Row(field1=2, field2="bar"))
            ]]
        )])
    df_nested_B.printSchema()
    
    root
     |-- arrayA: array (nullable = true)
     |    |-- element: array (containsNull = true)
     |    |    |-- element: struct (containsNull = true)
     |    |    |    |-- childStructB: struct (nullable = true)
     |    |    |    |    |-- field1: long (nullable = true)
     |    |    |    |    |-- field2: string (nullable = true)
     
Or this one

	df_nested_C = spark.createDataFrame([
        Row(
            arrayA=[
                Row(childStructB=Row(childArrayC=[Row(field1=1, field2="foo")])),
                Row(childStructB=Row(childArrayC=[Row(field1=2, field2="bar")])),
            ]
        )])
    df_nested_C.printSchema()
    
    root
     |-- arrayA: array (nullable = true)
     |    |-- element: struct (containsNull = true)
     |    |    |-- childStructB: struct (nullable = true)
     |    |    |    |-- childArrayC: array (nullable = true)
     |    |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |    |-- field1: long (nullable = true)
     |    |    |    |    |    |-- field2: string (nullable = true)
     
Selecting `arrayA.childStructB.field1` from `df_nested_B` fails with the error message: `AnalysisException: No such struct field field1 in childStructB`.<br>
While selecting `arrayA.childStructB.childArrayC.field1` from `df_nested_C` throws the `AnalysisException`: `cannot resolve 'arrayA.childStructB.childArrayC['field1']' due to data type mismatch: argument 2 requires integral type, however, ''field1'' is of string type.`

## (More) Solutions
With the introduction of the SQL function `transform` in Spark 2.4, the error above can be solved by applying `transform` on every layer of the array.

A comprehensive implementation of a flatten function can be found in the Python package `sparkaid`:

	from sparkaid import flatten
	
	flatten(df_nested_B).printSchema()
	
	root
	 |-- arrayA__childStructB_field1: array (nullable = true)
	 |    |-- element: array (containsNull = true)
	 |    |    |-- element: long (containsNull = true)
	 |-- arrayA__childStructB_field2: array (nullable = true)
	 |    |-- element: array (containsNull = true)
	 |    |    |-- element: string (containsNull = true)
<p>

	flatten(df_nested_B).show()
	
	+---------------------------+---------------------------+
    |arrayA__childStructB_field1|arrayA__childStructB_field2|
    +---------------------------+---------------------------+
    |                   [[1, 2]]|               [[foo, bar]]|
    +---------------------------+---------------------------+
<p>
  
    flatten(df_nested_C).printSchema()

    root
     |-- arrayA_childStructB_childArrayC_field1: array (nullable = true)
     |    |-- element: array (containsNull = true)
     |    |    |-- element: long (containsNull = true)
     |-- arrayA_childStructB_childArrayC_field2: array (nullable = true)
     |    |-- element: array (containsNull = true)
     |    |    |-- element: string (containsNull = true)

[]: https://spark.apache.org/docs/2.4.0/api/java/org/apache/spark/sql/functions.html#flatten-org.apache.spark.sql.Column
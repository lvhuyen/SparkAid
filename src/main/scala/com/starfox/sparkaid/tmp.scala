package com.starfox.sparkaid

import org.apache.spark.sql.types.{ArrayType, DataType, IntegerType, LongType, StructField, StructType}
import org.apache.spark.sql.DataFrame

import scala.collection.mutable


class tmp(val separator:String = "__", val arrayDenotation: String = "", val fieldNameNormalizer: String => String = x => x ) {
  private val QUOTE = ""
  private val actualArrayDenotation = arrayDenotation + separator

  private val safeFieldNameNormalizer: String => String = (raw: String) => {
    val ret = fieldNameNormalizer (raw)
    ret
  }

  def buildHiveCreateTableDdl(df: DataFrame, tableName: String, location: String, partitions: Seq[String]): String = {
    def toHiveSchemaString(fieldType: DataType, indentLevel: Int = 2): String = {
      val indent = "    " * indentLevel
      val dataType = fieldType match {
        case a: ArrayType => s"array<\n$indent${toHiveSchemaString(a.elementType, indentLevel + 1)}>"
        case s: StructType =>
          val children = s.fields.map(f => s"""`${f.name}`:${toHiveSchemaString(f.dataType, indentLevel + 1)}""").mkString(s",\n$indent")
          s"struct<\n$indent$children>"
        case _: IntegerType => s"int"
        case _: LongType => s"bigint"
        case _ => s"${fieldType.typeName}"
      }
      dataType
    }

    val schemaString = df.schema.fields.filter(field => !partitions.contains(field.name)).map(f => s"""`${f.name}` ${toHiveSchemaString(f.dataType)}""").mkString(",\n     ")
    val partitionsString = df.schema.fields.filter(field => partitions.contains(field.name)).map(f => s"""`${f.name}` ${toHiveSchemaString(f.dataType)}""").mkString(", ")

    s"""
       |CREATE EXTERNAL TABLE $tableName (\n
       |     $schemaString) \n
       | PARTITIONED BY ($partitionsString) \n
       | ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' \n
       | STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' \n
       | OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' \n
       | LOCATION '$location'
       """.stripMargin
  }

  def buildHiveFlattenQuery(df: DataFrame, rawTableName: String): String = {
    val fieldsInfo: Array[(Vector[Vector[String]], DataType)] = getFieldsInfoForFlattening(df.schema)
    val cols: Array[String] = fieldsInfo.map (f => f._1 match {
      case head +: Seq() =>
        if (head.length > 1) s"${buildQualifiedName(head)} AS ${buildFlattenedChunkName(head)}"
        else s"$QUOTE${head.head}$QUOTE"
      case head +: anotherField =>
        if (anotherField.head.isEmpty)
            s"ARR_${buildFlattenedChunkName(head)} AS ${buildFlattenedChunkName(head)}"
          else
            s"ARR_${buildFlattenedChunkName(head)}.${buildQualifiedName(anotherField.head)} AS ${buildFlattenedFieldName(Vector(head, anotherField.head))}"
    })

    def getHiveType(sparkType: DataType): String = sparkType.typeName match {
      case "string" => "varchar"
      case "long" => "bigint"
      case "integer" => "int"
      case _ => sparkType.typeName
    }

    //    def buildExplodeSchema(children: Array[(Vector[String], DataType)])(implicit fillWithNull: Boolean): String = {
    //      children.groupBy(_._1.head).map(g => g._2.length match {
    //        case 1 => if (fillWithNull) "null" else s"${g._1} ${getHiveType(g._2(0)._2)}"
    //        case _ => s"${if (fillWithNull) "" else g._1} ROW(${buildExplodeSchema(g._2.map(f => (f._1.tail, f._2)))})"
    //      }).mkString(", ")
    //    }

    def buildExplodeSchema(children: mutable.ArraySeq[(Vector[String], DataType)])(implicit fillWithNull: Boolean): String = {
      groupByWithOrderPreserved[(Vector[String], DataType), String](children)(_._1.head)
        .map(g => g._2.length match {
          case 1 => if (fillWithNull) "null" else s"${g._1} ${getHiveType(g._2(0)._2)}"
          case _ => s"${if (fillWithNull) "" else g._1} ROW(${buildExplodeSchema(g._2.map(f => (f._1.tail, f._2)))})"
        }).mkString(", ")
    }

    def groupByWithOrderPreserved[T, K](raw: mutable.ArraySeq[T])(groupFunction: T => K) = {
      val m = raw.zipWithIndex.groupBy(f => groupFunction(f._1))
      val lhm = mutable.LinkedHashMap(m.toSeq.sortBy(_._2.head._2): _*)
      lhm.mapValues(_.map(_._1))
    }

    //    def buildExplodeSchema(children: Array[(Vector[String], DataType)]): (String, String) = {
    //      val tmp: Iterable[(String, String)] = children.groupBy(_._1.head).map(g => g._2.length match {
    //        case 1 => ("null", s"${g._2(0)._1.tail} ${getHiveType(g._2(0)._2)}")
    //        case _ => buildExplodeSchema(g._2.map(f => (f._1.tail, f._2)))
    //      })
    //      (s"ROW(${tmp.map(_._1).mkString(", ")})", s"ROW(${tmp.map(_._2).mkString(", ")})")
    //    }

    val joinClauses1: Array[String] = fieldsInfo
      .filter(_._1.size == 2)
      .filter(_._1.tail.head.isEmpty)
      .map(f => {
        val qualifiedName = buildQualifiedName(f._1.head)
        s"""CROSS JOIN UNNEST (
           |  CASE
           |    WHEN cardinality($qualifiedName) > 0 then $qualifiedName
           |    ELSE ARRAY[null]
           |  END
           |) AS t(ARR_${buildFlattenedChunkName(f._1.head)})""".stripMargin
      })

    val joinClauses2: Array[String] = fieldsInfo
      .filter(f => f._1.size == 2)
      .filter(_._1.tail.head.nonEmpty)
      .map(f => (f._1.head, f._1.tail.head, f._2))
      .groupBy(_._1)
      .map(g => {
        val key: Vector[String] = g._1
        val children: mutable.ArraySeq[(Vector[String], DataType)] = g._2.map(f => (f._2, f._3))
        val qualifiedName = buildQualifiedName(key)

        val rowOfNull = buildExplodeSchema(children)(fillWithNull = true)
        val rowSchema = buildExplodeSchema(children)(fillWithNull = false)

        s"""CROSS JOIN UNNEST (
           |  CASE
           |    WHEN cardinality($qualifiedName) > 0 then $qualifiedName
           |    ELSE ARRAY[
           |      CAST(ROW($rowOfNull)
           |        AS ROW($rowSchema))]
           |  END
           |) AS t(ARR_${buildFlattenedChunkName(key)})""".stripMargin
      }).toArray

    s"""SELECT ${cols.mkString(",\n    ")}
       |FROM ${rawTableName}
       |${joinClauses1.mkString("\n")}
       |${joinClauses2.mkString("\n")}
       |""".stripMargin
  }

  /** This recursive function reads the nested schema, breaks down each root-leaf path into one 2-dimension collection of String.
   *  Each (inner) element in the collection is one chunk of the nested schema. That big collection is broken down into segments separated by an ArrayType.
   *  E.g:
   *    For the root-leaf path (structA -> structB -> structC -> stringD), the output element is a 2-d collection,
   *      with the outer layer has only one element, which in turn consists of 4 element
   *    For the root-leaf path (structA -> arrayB(struct) -> structC -> structD -> integerE), the output element is a 2-d collection,
   *      Depending on `explodeArrayType`, if true, then the outer layer has 2 elements, one has 2 elements (A, B) and the other has 3 (C, D, E)
   *      otherwise, the outer layer would have only one element: (A, B) */
  private def getFieldsInfoForFlattening(dtype: DataType, name: Vector[String] = Vector.empty, includeArray: Boolean = true): Array[(Vector[Vector[String]], DataType)] = {
    dtype match {
      case st: StructType =>
        st.fields.flatMap(field => getFieldsInfoForFlattening(field.dataType, Vector(field.name), includeArray).map (
          child => ((name ++ child._1.head) +: child._1.tail, child._2)))

      case ar: ArrayType =>
        if (includeArray)
          getFieldsInfoForFlattening(ar.elementType).map(child => (Vector(name) ++ child._1, child._2))
//          ar.elementType match {
//            case e @ (_: StructType | _: ArrayType) =>
//              getFieldsInfoForFlattening(e).map(child => (Vector(name) ++ child._1, child._2))
//            case e => Array((Vector(name), e))
//          }
        else Array((Vector(name) ++ Vector(Vector.empty[String]), ar))

      case _ => Array((Vector(name), dtype))
    }
  }

  private def buildFlattenedChunkName(raw: Vector[String], normalizeRootSegment: Boolean = true): String = {
    if (normalizeRootSegment)
      raw.map(safeFieldNameNormalizer).mkString(separator)
    else
      (raw.head +: raw.tail.map(safeFieldNameNormalizer)).mkString(separator)
  }

  private def buildFlattenedFieldName(raw: Vector[Vector[String]]): String = {
    raw.map(buildFlattenedChunkName(_)).mkString(actualArrayDenotation)
  }

  private def buildQualifiedName(raw: Vector[String]): String = raw.map(segment => f"$QUOTE$segment$QUOTE").mkString(".")
}

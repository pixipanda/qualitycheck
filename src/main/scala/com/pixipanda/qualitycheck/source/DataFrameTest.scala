package com.pixipanda.qualitycheck.source

import com.pixipanda.qualitycheck.check.Check
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{BooleanType, DataType, DoubleType, IntegerType, LongType, ShortType, StringType, StructField, StructType}

abstract class DataFrameTest(
  sourceType: String,
  dbName: String,
  tableName: String,
  query: String,
  checks: Seq[Check]) extends Source(sourceType, query){

  val data: Map[String, List[Any]] = Map(
    "item"     -> List("Eggs", "Milk", "Bread", "Cheese"),
    "price"    -> List(  5.49,   3.89,    4.50,     6.00),
    "quantity" -> List(    12,      5,       2,       10)
  )

  override  def getChecks: Seq[Check] = checks

  override def getQueryDF:DataFrame = mkDF(data.toSeq: _*)

  override def getDF:DataFrame = mkDF(data.toSeq: _*)

  override def getSourceType: String = sourceType

  override def exists: Boolean = {
      true
  }

  override def getLabel: String =
    if(null != query)
      s"${dbName}_${tableName}_query"
    else
      s"${dbName}_$tableName"


  def guessType(v: Any): DataType = v.getClass.getCanonicalName match {
    case "java.lang.Short" => ShortType
    case "java.lang.String" => StringType
    case "java.lang.Integer" => IntegerType
    case "java.lang.Double" => DoubleType
    case "java.lang.Boolean" => BooleanType
    case "java.lang.Long" => LongType
    case _ => throw new IllegalArgumentException(s"Unknown type '${v.getClass.getCanonicalName}'")
  }

  def mkSchema(args: (String, List[Any])*): StructType = {
    StructType(args.map(x => StructField(x._1, guessType(x._2.head))))
  }

  def mkRows(args: (String, List[Any])*): List[Row] = {
    val len = args.head._2.length
    require(args.forall(_._2.length == len))
    (0 until len).map(i => Row(args.map(_._2.apply(i)): _*)).toList
  }


  /**
    * creates dataFrame from array of (label, List[Any])
    * @param args - is array of tuple(String, List[Any]) supported types are String, Double, Int, Long
    * @return DataFrame
    *
    * ie mkDf(("item"     -> List("Eggs", "Milk", "Bread", "Cheese")),
    *         ("price"    -> List(  5.49,   3.89,    4.50,     6.00),
    *         ("quantity" -> List(    12,      5,       2,       10)))
    *
    * will return a dataframe
    *
    *
    */
  def mkDF(args: (String, List[Any])*): DataFrame = {
    require(args.forall(_._2.length == args.head._2.length))
    val schema = mkSchema(args: _*)
    val data = mkRows(args: _*)
    spark.createDataFrame(sc.parallelize(data), schema)
  }
}

package com.pixipanda.qualitycheck.source

import com.pixipanda.qualitycheck.check.Check
import com.pixipanda.qualitycheck.codec.JsonDecoder
import io.circe.{HCursor, Json}
import org.apache.spark.sql.DataFrame


case class Hive(
  sourceType: String,
  dbName: String,
  tableName: String,
  query: String,
  checks: Seq[Check]) extends Source(sourceType, query){


  override  def getChecks: Seq[Check] = checks

  override def getQueryDF:DataFrame = spark.sql(query)

  override def getDF:DataFrame = {
    if(null != query) {
      spark.sql(query)
    }else {
      spark.table(s"$dbName.$tableName")
    }
  }

  override def getSourceType: String = sourceType

  override def exists: Boolean = {
    spark.catalog.tableExists(dbName,tableName)
  }

  override def getLabel: String =
    if(null != query)
      s"${dbName}_${tableName}_query"
    else
      s"${dbName}_$tableName"

}

object Hive {

  def fromJson(hCursor: HCursor): Source = {
    val sourceType = hCursor.downField("type").as[String].right.get
    val dbName = hCursor.downField("dbName").as[String].right.get
    val tableName = hCursor.downField("tableName").as[String].right.get
    val query = hCursor.downField("query").as[String].right.get
    val checksJson = hCursor.downField("checks").as[Json].right.get // set it to list JSon but technically
    // because each itemJson downField will become List[Either[DecodeFailure, Int]] so we need to use cats to traverse List[Either] to Either[List]
    // each of as[Int] returns a Result[Int] and Result[Int] has a type of Either[DecodeFailure,A] which will become List[Either]
    val checks = JsonDecoder.decodeChecks(checksJson.hcursor)
    Hive(sourceType, dbName, tableName, query, checks)
  }
}

package com.pixipanda.qualitycheck.source.table

import com.pixipanda.qualitycheck.check.{Check, DistinctRelation}
import com.pixipanda.qualitycheck.config.Options
import com.pixipanda.qualitycheck.constant.DataStores._
import com.pixipanda.qualitycheck.source.Source
import com.typesafe.config.Config
import org.slf4j.{Logger, LoggerFactory}

abstract class Table(sourceType: String, query: String) extends Source(sourceType){

  def getDb: String

  def getTable: String

  def getQuery:String

  def getDistinctQueries(distinctCheck: Seq[DistinctRelation]): Seq[(DistinctRelation, String)] = {

    distinctCheck.map(dr =>  {
      val query = s"""
                     |SELECT COUNT(DISTINCT ${dr.columns.mkString(",")})
                     |FROM $getDb.$getTable
       """.stripMargin
      (dr, query)
    })
  }

  def rowCountQuery: String = {
    s"(select count(*) as count from $getDb.$getTable) t"
  }
}

object Table {

  val LOGGER: Logger = LoggerFactory.getLogger(getClass.getName)

  val checkONDF = true

  private def parserQuery(config: Config): String = {
    if(config.hasPath("query")) config.getString("query") else null
  }


  def parse(config: Config): Source =  {

    LOGGER.info("Parsing Table")
    LOGGER.debug(s"Parsing Table config: $config")

    val sourceType = config.getString("type")
    val dbName = config.getString("dbName")
    val tableName = config.getString("tableName")
    val query = parserQuery(config)
    val options = Options.parse(config)
    val checksConfig = config.getConfig("checks")
    val checks = Check.parse(checksConfig)

    LOGGER.info(s"Source type is $sourceType")

    sourceType match {
      case HIVE => Hive(sourceType, dbName, tableName, query, checkONDF, options, checks)
      case TERADATA => Teradata(sourceType, dbName, tableName, query, checkONDF, options, checks)
    }
  }
}
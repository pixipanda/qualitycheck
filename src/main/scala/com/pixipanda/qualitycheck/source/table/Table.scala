package com.pixipanda.qualitycheck.source.table

import com.pixipanda.qualitycheck.check.{Check, DistinctRelation}
import com.pixipanda.qualitycheck.config.Options
import com.pixipanda.qualitycheck.constant.DataStores._
import com.pixipanda.qualitycheck.source.Source
import com.typesafe.config.Config
import org.slf4j.{Logger, LoggerFactory}

abstract class Table(dbName: String, tableName: String, query: String) extends Source {

  def getDb: String = dbName

  def getTable: String = tableName

  def getQuery:String = query


  override def getLabel: String = {
    val common = s"$sourceType:$dbName:$tableName"
    if (null != query)
      s"$common:query"
    else
      s"$common"
  }

  def distinctQueries(distinctCheck: Seq[DistinctRelation]): Seq[(DistinctRelation, String)] = {

    distinctCheck.map(dr =>  {
      val query = s"""
                     |(SELECT COUNT(DISTINCT ${dr.columns.mkString(",")})
                     |FROM $getDb.$getTable) t
       """.stripMargin
      (dr, query)
    })
  }

  def rowCountQuery: String = {
    s"""
       |(SELECT COUNT(*) as count
       | FROM $getDb.$getTable) t
       | """.stripMargin
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
    val checksConfig = config.getConfig("checks")
    val checks = Check.parse(checksConfig)

    LOGGER.info(s"Source type is $sourceType")

    sourceType match {
      case HIVE => Hive(sourceType, dbName, tableName, query, checkONDF, checks)
      case MYSQL | ORACLE | POSTGRES | TERADATA =>
        val checkONDF = false
        val optionsConfig = config.getConfig("options")
        val options = Options.parse(optionsConfig)
        JDBC(sourceType, dbName, tableName, query, checkONDF, options, checks)
    }
  }
}
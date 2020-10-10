package com.pixipanda.qualitycheck.source.table

import com.pixipanda.qualitycheck.check.Check
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

  def distinctCheckQuery(columns: Seq[String]): String = {

    s"""
       |(SELECT COUNT(DISTINCT ${columns.mkString(",")}) as count
       | FROM $getDb.$getTable) t
       """.stripMargin

  }

  def rowCountCheckQuery: String = {
    s"""
       |(SELECT COUNT(*) as count
       | FROM $getDb.$getTable) t
       | """.stripMargin
  }

  def nullCheckQuery(column: String): String = {
    s"""
       |(SELECT COUNT(*) as count
       | FROM $getDb.$getTable
         WHERE $column IS NULL) t
       | """.stripMargin
  }

  def uniqueCheckQuery(columns: Seq[String]):String = {
    s"""
       |(SELECT COUNT(*) as count
       | FROM
       | ( SELECT ${columns.mkString(",")}, COUNT(*) as count
       |   FROM $dbName.$tableName
       |   GROUP BY ${columns.mkString(",")}
       |   HAVING COUNT(*) > 1
       | ) d
       |) t
     """.stripMargin
  }
}

object Table {

  val LOGGER: Logger = LoggerFactory.getLogger(getClass.getName)

  private def parserQuery(config: Config): String = {
    if(config.hasPath("query")) config.getString("query") else null
  }

  private def parsePredicatePush(config: Config) = {
    if(config.hasPath("predicatePush")) config.getString("predicatePush").toBoolean else false
  }

  def parse(config: Config): Source =  {

    LOGGER.info("Parsing Table")
    LOGGER.debug(s"Parsing Table config: $config")

    val sourceType = config.getString("type")
    val dbName = config.getString("dbName")
    val tableName = config.getString("tableName")
    val query = parserQuery(config)
    val checksConfig = config.getConfig("checks")
    val predicatePush = parsePredicatePush(config)
    val checks = Check.parse(checksConfig)

    LOGGER.info(s"Source type is $sourceType")

    sourceType match {

      case HIVE =>
        Hive(sourceType, dbName, tableName, query, predicatePush, checks)

      case MYSQL | ORACLE | POSTGRES | TERADATA =>
        if(!predicatePush) {
          LOGGER.warn(s"predicatePush is false for jdbc source $sourceType. This may not be efficient. Try setting predicatePush = true")
        }
        val optionsConfig = config.getConfig("options")
        val options = Options.parse(optionsConfig)
        JDBC(sourceType, dbName, tableName, query, predicatePush, options, checks)
    }
  }
}
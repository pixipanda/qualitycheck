package com.pixipanda.qualitycheck.source.table

import com.pixipanda.qualitycheck.check.Check
import com.pixipanda.qualitycheck.constant.DataStores._
import com.pixipanda.qualitycheck.source.Source
import com.typesafe.config.Config
import org.slf4j.{Logger, LoggerFactory}

abstract class Table(sourceType: String, query: String) extends Source(sourceType){

  def getDb: String

  def getTable: String

  def getQuery:String
}

object Table {

  val LOGGER: Logger = LoggerFactory.getLogger(getClass.getName)

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
      case HIVE => Hive(sourceType, dbName, tableName, query, checks)
      case TERADATA => Teradata(sourceType, dbName, tableName, query, checks)
    }
  }
}
package com.pixipanda.qualitycheck.source.table

import com.pixipanda.qualitycheck.check.Check
import com.pixipanda.qualitycheck.constant.DataStores._
import com.pixipanda.qualitycheck.source.Source
import com.typesafe.config.Config

abstract class Table(sourceType: String, query: String) extends Source(sourceType){

  def getDb: String

  def getTable: String

  def getQuery:String
}

object Table {

  private def parserQuery(config: Config): String = {
    if(config.hasPath("query")) config.getString("query") else null
  }

  def parse(config: Config): Source =  {
    val sourceType = config.getString("type")
    val dbName = config.getString("dbName")
    val tableName = config.getString("tableName")
    val query = parserQuery(config)
    val checksConfig = config.getConfig("checks")
    val checks = Check.parse(checksConfig)

    sourceType match {
      case HIVE => Hive(sourceType, dbName, tableName, query, checks)
      case TERADATA => Teradata(sourceType, dbName, tableName, query, checks)
    }
  }
}
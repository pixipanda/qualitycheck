package com.pixipanda.qualitycheck.source

import com.pixipanda.qualitycheck.Spark
import com.pixipanda.qualitycheck.check.Check
import com.pixipanda.qualitycheck.constant.DataStores._
import com.pixipanda.qualitycheck.source.file.FileSource
import com.pixipanda.qualitycheck.source.table.Table
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.slf4j.{Logger, LoggerFactory}


abstract class Source(sourceType: String) extends  Spark {

  def getChecks: Seq[Check]

  def getDF:DataFrame

  def getSourceType: String

  def exists: Boolean

  def getLabel: String
}

object Source{

  val LOGGER: Logger = LoggerFactory.getLogger(getClass.getName)

  def parse(config: Config): Source = {
    val sourceType = config.getString("type")
    sourceType match {
      case
        HIVE |
        TERADATA => Table.parse(config)
      case
        CSV |
        XML |
        JSON |
        ORC |
        PARQUET => FileSource.parse(config)
      case _ =>
        LOGGER.error(s"Unknown DataStores $sourceType in config!")
        throw new RuntimeException(s"Unknown DataStores in config $sourceType")
    }
  }
}
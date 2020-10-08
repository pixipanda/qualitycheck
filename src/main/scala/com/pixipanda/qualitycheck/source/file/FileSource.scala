package com.pixipanda.qualitycheck.source.file

import java.io.File

import com.pixipanda.qualitycheck.check.Check
import com.pixipanda.qualitycheck.config.Options
import com.pixipanda.qualitycheck.source.Source
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.slf4j.{Logger, LoggerFactory}

final case class FileSource(sourceType: String, checkOnDF: Boolean, options: Option[Map[String,String]], checks: Seq[Check]) extends Source(sourceType) {


  override def getChecks: Seq[Check] = checks

  override def getDF: DataFrame = {
    spark.read
      .format(fileOptions("format"))
      .options(fileOptions)
      .load(fileOptions("path"))
  }

  override def getSourceType: String = sourceType

  override def exists: Boolean = {
    val fileOptions = options.get
    val file = new File(fileOptions("path"))
    file.exists()
  }

  override def getLabel: String = {
    val file = new File(fileOptions("path"))
    val fileName = file.getName
    s"$sourceType:$fileName"
  }

  private def fileOptions = options.get
}

object  FileSource {

  val LOGGER: Logger = LoggerFactory.getLogger(getClass.getName)

  def parse(config: Config): Source =  {
    LOGGER.info(s"Parsing file config")
    LOGGER.debug(s"Parsing file config: $config")

    val checkOnDF = true

    val sourceType = config.getString("type")
    val options = Options.parse(config)
    val checksConfig = config.getConfig("checks")
    val checks = Check.parse(checksConfig)
    FileSource(sourceType, checkOnDF, options, checks)
  }
}
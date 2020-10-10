package com.pixipanda.qualitycheck.source.file

import java.io.File

import com.pixipanda.qualitycheck.check.Check
import com.pixipanda.qualitycheck.config.Options
import com.pixipanda.qualitycheck.source.Source
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.slf4j.{Logger, LoggerFactory}

final case class FileSource(sourceType: String, predicatePush: Boolean, options: Map[String,String], checks: Seq[Check]) extends Source {


  override def getDF: DataFrame = {
    spark.read
      .format(options("format"))
      .options(options)
      .load(options("path"))
  }


  override def exists: Boolean = {
    val file = new File(options("path"))
    file.exists()
  }

  override def getLabel: String = {
    val file = new File(options("path"))
    val fileName = file.getName
    s"$sourceType:$fileName"
  }
}

object  FileSource {

  val LOGGER: Logger = LoggerFactory.getLogger(getClass.getName)

  def parse(config: Config): Source =  {
    LOGGER.info(s"Parsing file config")
    LOGGER.debug(s"Parsing file config: $config")

    val predicatePush = false

    val sourceType = config.getString("type")
    val optionsConfig = config.getConfig("options")
    val options = Options.parse(optionsConfig)
    val checksConfig = config.getConfig("checks")
    val checks = Check.parse(checksConfig)
    FileSource(sourceType, predicatePush, options, checks)
  }
}
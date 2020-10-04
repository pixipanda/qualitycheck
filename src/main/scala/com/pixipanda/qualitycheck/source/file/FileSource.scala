package com.pixipanda.qualitycheck.source.file

import java.io.File

import com.pixipanda.qualitycheck.check.Check
import com.pixipanda.qualitycheck.config.Options
import com.pixipanda.qualitycheck.source.Source
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame

final case class FileSource(sourceType: String, options: Map[String,String], checks: Seq[Check]) extends Source(sourceType) {

  override def getChecks: Seq[Check] = checks

  override def getDF: DataFrame = {
    spark.read
      .format(options("format"))
      .options(options)
      .load(options("path"))
  }

  override def getSourceType: String = sourceType

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

  def parse(config: Config): Source =  {
    val sourceType = config.getString("type")
    val optionsConfig = config.getConfig("options")
    val options = Options.parse(optionsConfig)
    val checksConfig = config.getConfig("checks")
    val checks = Check.parse(checksConfig)
    FileSource(sourceType, options, checks)
  }
}
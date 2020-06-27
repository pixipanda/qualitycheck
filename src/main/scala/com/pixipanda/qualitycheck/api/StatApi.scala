package com.pixipanda.qualitycheck.api

import cats.syntax.either._
import com.pixipanda.qualitycheck.QualityCheckConfig
import com.pixipanda.qualitycheck.config.ConfigParser
import com.pixipanda.qualitycheck.report.ReportBuilder
import com.pixipanda.qualitycheck.utils.ComputeChecks
import com.typesafe.config.Config
import io.circe._
import org.apache.spark.sql.DataFrame

object StatApi {


  private def buildStats(qualityCheckConfig: QualityCheckConfig):DataFrame = {
    val result = ComputeChecks.runChecks(qualityCheckConfig)
    val sourceStat = result.stats
    ReportBuilder.buildReport(sourceStat)

  }

  def saveAsJson(config:Config, outputPath: String):Unit = {
    ConfigParser.parseConfig(config)
      .map(buildStats)
      .foreach(_.write.option("header", "true").json(outputPath))

  }

  def saveAsCsv(config:Config, outputPath: String):Unit = {
    ConfigParser.parseConfig(config)
      .map(buildStats)
      .foreach(_.write.option("header", "true").csv(outputPath))
  }

  def getStats(config: Config): Either[Error, DataFrame] = {
    ConfigParser.parseConfig(config)
      .map(buildStats)
  }
}

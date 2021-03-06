package com.pixipanda.qualitycheck.api

import cats.syntax.either._
import com.pixipanda.qualitycheck.QualityCheckConfig
import com.pixipanda.qualitycheck.compute.ComputeChecks
import com.pixipanda.qualitycheck.config.ConfigParser
import com.pixipanda.qualitycheck.report.ReportBuilder
import com.typesafe.config.Config
import io.circe._
import org.apache.spark.sql.DataFrame

object StatApi {


  private def buildStats(qualityCheckConfig: QualityCheckConfig):DataFrame = {
    val qualityCheckConfig = ConfigParser.parseQualityCheck()
    val sourcesStat = ComputeChecks.runChecks(qualityCheckConfig.sources)
    ReportBuilder.buildReportDF(sourcesStat)
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

package com.pixipanda.qualitycheck

import com.pixipanda.qualitycheck.compute.ComputeChecks
import com.pixipanda.qualitycheck.config.ConfigParser
import com.pixipanda.qualitycheck.report.ReportBuilder
import com.typesafe.scalalogging.LazyLogging

object Main extends LazyLogging {

  def main(args: Array[String]): Unit = {

    val outputStatsFile = args(0)

    val qualityCheckConfig = ConfigParser.parseQualityCheck()
    val sourcesStat = ComputeChecks.runChecks(qualityCheckConfig.sources)
    val isSuccess = sourcesStat.forall(_.isSuccess)
    if(!isSuccess) {
      logger.error("QualityCheck Failed")
      logger.info("outputStatsFile: " + outputStatsFile)
      val reportDF = ReportBuilder.buildReportDF(sourcesStat)
      ReportBuilder.saveReport(reportDF, outputStatsFile)
    }
  }
}

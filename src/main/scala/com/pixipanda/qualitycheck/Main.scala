package com.pixipanda.qualitycheck

import com.pixipanda.qualitycheck.compute.ComputeChecks
import com.pixipanda.qualitycheck.config.ConfigParser
import com.pixipanda.qualitycheck.report.ReportBuilder
import com.typesafe.scalalogging.LazyLogging

object Main extends LazyLogging {

  def main(args: Array[String]): Unit = {

    val outputStatsFile = args(0)

    val qualityCheckConfig = ConfigParser.parseQualityCheck()
    val result = ComputeChecks.runChecks(qualityCheckConfig.sources)
    val sourceStats = result.stats
    val reportDF = ReportBuilder.buildReport(sourceStats)
    if (!sourceStats.last.isSuccess) {
      logger.error("QualityCheck Failed")
      logger.info("outputStatsFile: " + outputStatsFile)
      ReportBuilder.saveReport(reportDF, outputStatsFile)
      System.exit(1)
    }
  }
}

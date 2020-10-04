package com.pixipanda.qualitycheck.jobs


import com.pixipanda.qualitycheck.compute.ComputeChecks
import com.pixipanda.qualitycheck.config.ConfigParser
import com.pixipanda.qualitycheck.report.ReportBuilder
import com.typesafe.scalalogging.LazyLogging


object QualityCheck extends LazyLogging {

  def main(args: Array[String]): Unit = {

    ConfigParser.parse() match {
      case Right(qualityCheckConfig) =>
        val result = ComputeChecks.runChecks(qualityCheckConfig.sources)
        val sourceStats = result.stats
        val report = ReportBuilder.buildReport(sourceStats)
        report.show(false)
        if(!sourceStats.last.isSuccess) {
          logger.error("QualityCheck Failed")
        }
      case Left(er) => logger.error(s"Failed to parse config file, $er")
    }
  }
}

package com.pixipanda.qualitycheck

import java.io.FileInputStream
import java.util.Properties

import com.pixipanda.qualitycheck.compute.ComputeChecks
import com.pixipanda.qualitycheck.config.ConfigParser
import com.pixipanda.qualitycheck.report.ReportBuilder
import org.apache.log4j.PropertyConfigurator
import org.slf4j.{Logger, LoggerFactory}


object Main {

  val LOGGER: Logger = LoggerFactory.getLogger(getClass.getName)


  def configLogging(path: String): Unit = {
    val props = new Properties()
    props.loadFromXML(new FileInputStream(path))
    PropertyConfigurator.configure(props)
    LOGGER.info("Logging configured!")
  }


  def main(args: Array[String]): Unit = {

    val outputStatsFile = args(0)
    val loggerConfigFile = args(1)

    configLogging(loggerConfigFile)

    val qualityCheckConfig = ConfigParser.parseQualityCheck()
    val sourcesStat = ComputeChecks.runChecks(qualityCheckConfig.sources)
    val isSuccess = sourcesStat.forall(_.isSuccess)
    if(!isSuccess) {
      LOGGER.error("QualityCheck Failed")
      LOGGER.info("outputStatsFile: " + outputStatsFile)
      val reportDF = ReportBuilder.buildReportDF(sourcesStat)
      ReportBuilder.saveReport(reportDF, outputStatsFile)
    }
  }
}

package com.pixipanda.qualitycheck.report

import com.pixipanda.qualitycheck.Spark
import com.pixipanda.qualitycheck.stat.sourcestat.SourceStat
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._
import org.slf4j.{Logger, LoggerFactory}


object ReportBuilder extends Spark{

  val LOGGER: Logger = LoggerFactory.getLogger(getClass.getName)

  def buildReport(sourcesStat: Seq[SourceStat]): Seq[SourceStatReport] = {
    sourcesStat.map(buildReport)
  }


  def buildReportDF(sourcesStat: Seq[SourceStat]): DataFrame = {
    val sourcesStatReport = sourcesStat.map(buildReport)
    createDF(sourcesStatReport)
  }


  def createDF(sourceStatReport: Seq[SourceStatReport]): DataFrame = {

    LOGGER.info("Creating Report DataFrame")
    import spark.implicits._

    val sourceStatReportDF = sourceStatReport.toDF()
    val checksStatReportDF = sourceStatReportDF.select($"label", explode($"checksStatReport").as("checksStatReport"))
    val columnsStatReportDF = checksStatReportDF.select($"label", explode($"checksStatReport.columnsStatReport").as("columnsStatReport"))
    val qualityCheckReportDF = columnsStatReportDF
      .select("label", "columnsStatReport.*")
      .withColumn("jobRunDateTime", current_timestamp())
    qualityCheckReportDF
  }


  def buildReport(sourceStat: SourceStat): SourceStatReport = {
    LOGGER.info(s"Building Report for the source: ${sourceStat.label}")
    val checkStatReport =  sourceStat.stats.map(_.getReportStat)
    SourceStatReport(sourceStat.label, checkStatReport)
  }



  def saveReport(df: DataFrame, path: String): Unit = {
    df.coalesce(1).write.mode(SaveMode.Overwrite).csv(path)
  }
}

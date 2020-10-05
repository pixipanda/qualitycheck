package com.pixipanda.qualitycheck.report

import com.pixipanda.qualitycheck.Spark
import com.pixipanda.qualitycheck.stat.sourcestat.SourceStat
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._


object ReportBuilder extends Spark{

  def buildReport(sourcesStat: Seq[SourceStat]): Seq[SourceStatReport] = {
    sourcesStat.map(buildReport)
  }


  def buildReportDF(sourcesStat: Seq[SourceStat]): DataFrame = {
    val sourcesStatReport = sourcesStat.map(buildReport)
    createDF(sourcesStatReport)
  }


  def createDF(sourceStatReport: Seq[SourceStatReport]): DataFrame = {

    import spark.implicits._

    val sourceStatReportDF = sourceStatReport.toDF()
    val checksStatReportDF = sourceStatReportDF.select($"label", explode($"checksStatReport").as("checksStatReport"))
    val columnsStatReportDF = checksStatReportDF.select($"label", explode($"checksStatReport.columnsStatReport").as("columnsStatReport"))
    val qualityCheckDF = columnsStatReportDF.select("label", "columnsStatReport.*")
    qualityCheckDF
  }


  def buildReport(sourceStat: SourceStat): SourceStatReport = {
    val checkStatReport =  sourceStat.stats.map(_.getReportStat)
    SourceStatReport(sourceStat.label, checkStatReport)
  }



  def saveReport(df: DataFrame, path: String): Unit = {
    df.coalesce(1).write.mode(SaveMode.Overwrite).csv(path)
  }
}

package com.pixipanda.qualitycheck.report

import com.pixipanda.qualitycheck.Spark
import com.pixipanda.qualitycheck.stat.sourcestat.SourceStat
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer

object ReportBuilder extends Spark{

  def buildReport(sourceStats: Seq[SourceStat]): DataFrame = {

    val rows = ListBuffer[SourceStatReport]()

    sourceStats.foreach(sourceStat => {
      sourceStat.stats.foreach(checkStat => {
        val checkStatReport = checkStat.getReportStat
        checkStatReport.foreach(report => {
          rows.append(SourceStatReport(sourceStat.label, report))
        })
      })
    })

    import spark.implicits._
    val reportDF = rows.toList.toDF
    val qualityStatReport = reportDF
      .select("label", "checkReport.*")
      .withColumn("jobRunDate", current_date())

    qualityStatReport
  }


  def saveReport(df: DataFrame, path: String): Unit = {
    df.show(false)
    df.coalesce(1).write.mode(SaveMode.Overwrite).csv(path)
  }
}

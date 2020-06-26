package com.pixipanda.qualitycheck.stat.checkstat

import com.pixipanda.qualitycheck.constant.Stats.NULLSTAT
import com.pixipanda.qualitycheck.report.CheckStatReport

import scala.collection.mutable.ListBuffer

case class NullStat(
  statMap: Map[String, Long]
  ) extends  CheckStat(false) {

  override def getReportStat: Seq[CheckStatReport] = {
    val stats = ListBuffer[CheckStatReport]()
    statMap.foreach({
      case (column, actual) =>
        val reportStat = CheckStatReport(NULLSTAT,
          column,
          "eq",
          "0",
          actual.toString,
          this.getValidation
        )
        stats.append(reportStat)
    })
    stats.toList
  }
}

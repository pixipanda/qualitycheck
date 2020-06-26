package com.pixipanda.qualitycheck.stat.checkstat

import com.pixipanda.qualitycheck.check.RowCountCheck
import com.pixipanda.qualitycheck.constant.Stats._
import com.pixipanda.qualitycheck.report.CheckStatReport

import scala.collection.mutable.ListBuffer


case class RowCountStat(
  statMap: Map[RowCountCheck, Long]
) extends  CheckStat(false) {

  override def getReportStat: Seq[CheckStatReport] = {
    val stats = ListBuffer[CheckStatReport]()
    statMap.foreach({
      case (rowCountCheck, actual) =>
        val reportStat = CheckStatReport(ROWCOUNTSTAT,
          "NA",
          rowCountCheck.relation,
          rowCountCheck.count.toString,
          actual.toString,
          this.getValidation
        )
        stats.append(reportStat)
    })
    stats.toList
  }
}

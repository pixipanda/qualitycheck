package com.pixipanda.qualitycheck.stat.checkstat

import com.pixipanda.qualitycheck.constant.Stats.UNIQUESTAT
import com.pixipanda.qualitycheck.report.{CheckStatReport, ColumnStatReport}


case class UniqueStat(statMap: Map[String, Long], isSuccess: Boolean = false) extends  CheckStat {

  override def getReportStat: CheckStatReport = {
    val columnsStatReport = statMap.map({
      case (column, actual) =>
        ColumnStatReport(UNIQUESTAT,
          column,
          "eq",
          "0",
          actual.toString,
          this.getValidation
        )
    }).toList
    CheckStatReport(columnsStatReport)
  }

  /*
   * This function validates Uniques Stats.
   * If there are duplicate records for the give set of columns then returns false else returns true
   */
  override def validate: CheckStat = {
    val status = this.statMap.forall{
      case (_, count) => count == 0
    }
    UniqueStat(this.statMap, status)
  }
}

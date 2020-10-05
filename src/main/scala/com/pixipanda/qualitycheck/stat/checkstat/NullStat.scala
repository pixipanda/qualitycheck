package com.pixipanda.qualitycheck.stat.checkstat

import com.pixipanda.qualitycheck.constant.Stats.NULLSTAT
import com.pixipanda.qualitycheck.report.{CheckStatReport, ColumnStatReport}


case class NullStat(statMap: Map[String, Long], isSuccess: Boolean = false) extends  CheckStat {

  override def getReportStat: CheckStatReport = {

    val columnsStatReport = statMap.map({
      case (column, actual) =>
        ColumnStatReport(NULLSTAT,
          column,
          "gt",
          "0",
          actual.toString,
          this.getValidation
        )
    }).toList
    CheckStatReport(columnsStatReport)
  }

  /*
   * This function validates Null Stats.
   * If the computed stats does not match the config then returns false else returns true
   */
  override def validate: CheckStat = {

    val nullStatMap = this.statMap

    val status = nullStatMap.forall {
      case (_, count) => count == 0
    }
    NullStat(nullStatMap, status)
  }
}

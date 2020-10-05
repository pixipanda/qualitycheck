package com.pixipanda.qualitycheck.stat.checkstat

import com.pixipanda.qualitycheck.check.RowCountCheck
import com.pixipanda.qualitycheck.constant.Stats._
import com.pixipanda.qualitycheck.report.{CheckStatReport, ColumnStatReport}



case class RowCountStat(statMap: Map[RowCountCheck, Long], isSuccess: Boolean = false) extends  CheckStat {

  override def getReportStat: CheckStatReport = {
    val rowCountStatReport = statMap.map({
      case (rowCountCheck, actual) =>
        ColumnStatReport(ROWCOUNTSTAT,
          "NA",
          rowCountCheck.relation,
          rowCountCheck.count.toString,
          actual.toString,
          this.getValidation
        )
    }).toList
    CheckStatReport(rowCountStatReport)
  }

  /*
    This function validates RowCount Stats.
    If the computed stats does not match the config then returns false else returns true
   */
  override def validate:CheckStat = {

    val rowCountStatMap = this.statMap

    val status = rowCountStatMap.forall{
      case(rowCountConfig, actual) =>
        rowCountConfig.relation match {
          case "gt" => actual > rowCountConfig.count
          case "ge" => actual >= rowCountConfig.count
          case "lt" => actual < rowCountConfig.count
          case "le" => actual <= rowCountConfig.count
          case "eq" => actual == rowCountConfig.count
        }
    }
    RowCountStat(rowCountStatMap, status)
  }
}

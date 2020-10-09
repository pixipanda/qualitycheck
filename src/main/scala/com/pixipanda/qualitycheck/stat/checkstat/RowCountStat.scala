package com.pixipanda.qualitycheck.stat.checkstat

import com.pixipanda.qualitycheck.check.RowCountCheck
import com.pixipanda.qualitycheck.constant.Stats._
import com.pixipanda.qualitycheck.report.{CheckStatReport, ColumnStatReport}
import org.slf4j.{Logger, LoggerFactory}



case class RowCountStat(statMap: Map[RowCountCheck, (Long, Boolean)]) extends  CheckStat {

  val LOGGER: Logger = LoggerFactory.getLogger(getClass.getName)

  override def getReportStat: CheckStatReport = {

    LOGGER.info(s"Creating ROWCOUNTSTAT")

    val rowCountStatReport = statMap.map({
      case (rowCountCheck, (actual, isSuccess)) =>
        ColumnStatReport(ROWCOUNTSTAT,
          "NA",
          rowCountCheck.relation,
          rowCountCheck.count.toString,
          actual.toString,
          if(isSuccess)  "success" else "failed"
        )
    }).toList
    CheckStatReport(rowCountStatReport)
  }

  /*
    This function validates RowCount Stats.
    If the computed stats does not match the config then returns false else returns true
   */
  override def validate:CheckStat = {

    LOGGER.info(s"Validating ROWCOUNTSTAT")

    val rowCountStatMap = this.statMap


    val validatedStatMap = rowCountStatMap.map(keyValue => {
      val rowCountConfig = keyValue._1
      val (actual, _) = keyValue._2
      rowCountConfig.relation match {
        case "gt" => if(actual > rowCountConfig.count) rowCountConfig -> (actual, true) else  keyValue
        case "ge" => if(actual >= rowCountConfig.count) rowCountConfig -> (actual, true) else  keyValue
        case "lt" => if(actual < rowCountConfig.count) rowCountConfig -> (actual, true) else  keyValue
        case "le" => if(actual >= rowCountConfig.count) rowCountConfig -> (actual, true) else  keyValue
        case "eq" => if(actual == rowCountConfig.count) rowCountConfig -> (actual, true) else  keyValue
      }
    })
    RowCountStat(validatedStatMap)
  }

  override def isSuccess: Boolean = {
    this.statMap.forall({
      case (_, (_, isCheckSuccess)) => isCheckSuccess
    })
  }
}

package com.pixipanda.qualitycheck.stat.checkstat

import com.pixipanda.qualitycheck.check.DistinctRelation
import com.pixipanda.qualitycheck.constant.Stats.DISTINCTSTAT
import com.pixipanda.qualitycheck.report.{CheckStatReport, ColumnStatReport}



case class DistinctStat(statMap: Map[DistinctRelation, Long], isSuccess: Boolean = false) extends  CheckStat {

  override def getReportStat: CheckStatReport = {

    val columnsStatReport = statMap.map({
      case (distinctRelation: DistinctRelation, actual) =>
        ColumnStatReport(
          DISTINCTSTAT,
          distinctRelation.columns.mkString(":"),
          distinctRelation.relation,
          distinctRelation.count.toString,
          actual.toString,
          this.getValidation
        )
    }).toList

    CheckStatReport(columnsStatReport)
  }

  /*
   * This function validates Distinct Stats.
   * If the computed stats does not match the config then returns false else returns true
   */
  override def validate:CheckStat = {

    val distinctStatMap = this.statMap

    val status = distinctStatMap.forall{
      case(distinctConfig, actual) =>
        distinctConfig.relation match {
          case "gt" => actual > distinctConfig.count
          case "ge" => actual >= distinctConfig.count
          case "lt" => actual < distinctConfig.count
          case "le" => actual <= distinctConfig.count
          case "eq" => actual == distinctConfig.count
        }
    }
    DistinctStat(distinctStatMap, status)
  }

}


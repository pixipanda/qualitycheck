package com.pixipanda.qualitycheck.stat.checkstat

import com.pixipanda.qualitycheck.check.DistinctRelation
import com.pixipanda.qualitycheck.constant.Stats.DISTINCTSTAT
import com.pixipanda.qualitycheck.report.{CheckStatReport, ColumnStatReport}
import org.slf4j.{Logger, LoggerFactory}



case class DistinctStat(statMap: Map[DistinctRelation, (Long, Boolean)]) extends  CheckStat {

  val LOGGER: Logger = LoggerFactory.getLogger(getClass.getName)

  override def getReportStat: CheckStatReport = {

    LOGGER.info(s"Creating DISTINCTSTAT Report")
    val columnsStatReport = statMap.map({
      case (distinctRelation: DistinctRelation, (actual, isSuccess)) =>
        ColumnStatReport(
          DISTINCTSTAT,
          distinctRelation.columns.mkString(":"),
          distinctRelation.relation,
          distinctRelation.count.toString,
          actual.toString,
          if(isSuccess)  "success" else "failed"
        )
    }).toList

    CheckStatReport(columnsStatReport)
  }

  /*
   * This function validates Distinct Stats.
   * If the computed stats does not match the config then returns false else returns true
   */
  override def validate:CheckStat = {

    LOGGER.info(s"Validating DISTINCTSTAT")

    val distinctStatMap = this.statMap


    val validatedStatMap = distinctStatMap.map(keyValue => {
      val distinctRelation = keyValue._1
      val (actual, _) = keyValue._2
      distinctRelation.relation match {
        case "gt" => if(actual > distinctRelation.count)  distinctRelation -> (actual, true) else keyValue
        case "ge" => if(actual >= distinctRelation.count) distinctRelation -> (actual, true) else keyValue
        case "lt" => if(actual < distinctRelation.count)  distinctRelation -> (actual, true) else keyValue
        case "le" => if(actual <= distinctRelation.count) distinctRelation -> (actual, true) else keyValue
        case "eq" => if(actual == distinctRelation.count) distinctRelation -> (actual, true) else keyValue
      }
    })
    DistinctStat(validatedStatMap)
  }


  override def isSuccess: Boolean = {
    this.statMap.forall({
      case (_, (_, isCheckSuccess)) => isCheckSuccess
    })
  }
}


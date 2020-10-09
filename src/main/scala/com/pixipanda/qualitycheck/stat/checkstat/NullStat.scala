package com.pixipanda.qualitycheck.stat.checkstat

import com.pixipanda.qualitycheck.constant.Stats.NULLSTAT
import com.pixipanda.qualitycheck.report.{CheckStatReport, ColumnStatReport}
import org.slf4j.{Logger, LoggerFactory}


case class NullStat(statMap: Map[String, (Long, Boolean)]) extends  CheckStat {

  val LOGGER: Logger = LoggerFactory.getLogger(getClass.getName)

  override def getReportStat: CheckStatReport = {

    LOGGER.info(s"Creating NULLSTAT")

    val columnsStatReport = statMap.map({
      case (column, (actual, isSuccess)) =>
        ColumnStatReport(NULLSTAT,
          column,
          "gt",
          "0",
          actual.toString,
          if(isSuccess)  "success" else "failed"
        )
    }).toList
    CheckStatReport(columnsStatReport)
  }

  /*
   * This function validates Null Stats.
   * If the computed stats does not match the config then returns false else returns true
   */
  override def validate: CheckStat = {

    LOGGER.info(s"Validating NULLSTAT")

    val nullStatMap = this.statMap

    val validatedStatMap = nullStatMap.map(keyValue => {
      val column = keyValue._1
      val (nullCount, _) = keyValue._2
      if(nullCount == 0) column -> (nullCount, true) else  keyValue
    })
    NullStat(validatedStatMap)
  }

  override def isSuccess: Boolean = {
    this.statMap.forall({
      case (_, (_, isCheckSuccess)) => isCheckSuccess
    })
  }
}

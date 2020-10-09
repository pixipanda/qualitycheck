package com.pixipanda.qualitycheck.stat.checkstat

import com.pixipanda.qualitycheck.constant.Stats.UNIQUESTAT
import com.pixipanda.qualitycheck.report.{CheckStatReport, ColumnStatReport}
import org.slf4j.{Logger, LoggerFactory}


case class UniqueStat(statMap: Map[String, (Long, Boolean)]) extends  CheckStat {

  val LOGGER: Logger = LoggerFactory.getLogger(getClass.getName)

  override def getReportStat: CheckStatReport = {

    LOGGER.info(s"Creating UNIQUESTAT")

    val columnsStatReport = statMap.map({
      case (column, (actual, isSuccess)) =>
        ColumnStatReport(UNIQUESTAT,
          column,
          "eq",
          "0",
          actual.toString,
          if(isSuccess)  "success" else "failed"
        )
    }).toList
    CheckStatReport(columnsStatReport)
  }

  /*
   * This function validates Uniques Stats.
   * If there are duplicate records for the give set of columns then returns false else returns true
   */
  override def validate: CheckStat = {

    LOGGER.info(s"Validating UNIQUESTAT")

    val validatedStat = this.statMap.map(keyValue =>{
      val columns = keyValue._1
      val (duplicateCount, _) = keyValue._2
      if(duplicateCount == 0) columns -> (duplicateCount, true) else keyValue
    })

    UniqueStat(validatedStat)
  }

  override def isSuccess: Boolean = {
    this.statMap.forall({
      case (_, (_, isCheckSuccess)) => isCheckSuccess
    })
  }
}

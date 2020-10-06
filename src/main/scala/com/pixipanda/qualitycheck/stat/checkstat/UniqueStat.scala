package com.pixipanda.qualitycheck.stat.checkstat

import com.pixipanda.qualitycheck.constant.Stats.UNIQUESTAT
import com.pixipanda.qualitycheck.report.{CheckStatReport, ColumnStatReport}
import org.slf4j.{Logger, LoggerFactory}


case class UniqueStat(statMap: Map[String, Long], isSuccess: Boolean = false) extends  CheckStat {

  val LOGGER: Logger = LoggerFactory.getLogger(getClass.getName)

  override def getReportStat: CheckStatReport = {

    LOGGER.info(s"Creating UNIQUESTAT")

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

    LOGGER.info(s"Validating UNIQUESTAT")

    val status = this.statMap.forall{
      case (_, count) => count == 0
    }
    UniqueStat(this.statMap, status)
  }
}

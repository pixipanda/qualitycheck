package com.pixipanda.qualitycheck.stat.checkstat

import com.pixipanda.qualitycheck.constant.Stats.NULLSTAT
import com.pixipanda.qualitycheck.report.{CheckStatReport, ColumnStatReport}
import org.slf4j.{Logger, LoggerFactory}


case class NullStat(statMap: Map[String, Long], isSuccess: Boolean = false) extends  CheckStat {

  val LOGGER: Logger = LoggerFactory.getLogger(getClass.getName)

  override def getReportStat: CheckStatReport = {

    LOGGER.info(s"Creating NULLSTAT")

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

    LOGGER.info(s"Validating NULLSTAT")

    val nullStatMap = this.statMap

    val status = nullStatMap.forall {
      case (_, count) => count == 0
    }
    NullStat(nullStatMap, status)
  }
}

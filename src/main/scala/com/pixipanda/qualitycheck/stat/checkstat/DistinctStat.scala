package com.pixipanda.qualitycheck.stat.checkstat

import com.pixipanda.qualitycheck.check.DistinctRelation
import com.pixipanda.qualitycheck.constant.Stats.DISTINCTSTAT
import com.pixipanda.qualitycheck.report.CheckStatReport

import scala.collection.mutable.ListBuffer


case class DistinctStat(
  statMap: Map[DistinctRelation, Long]
) extends  CheckStat(false) {

  override def getReportStat: Seq[CheckStatReport] = {
    val stats = ListBuffer[CheckStatReport]()

    statMap.foreach({
      case (distinctRelation, actual) =>
        val reportStat = CheckStatReport(DISTINCTSTAT,
          distinctRelation.columns.mkString(":"),
          distinctRelation.relation,
          distinctRelation.count.toString,
          actual.toString,
          this.getValidation)
        stats.append(reportStat)
    })
    stats.toList
  }
}


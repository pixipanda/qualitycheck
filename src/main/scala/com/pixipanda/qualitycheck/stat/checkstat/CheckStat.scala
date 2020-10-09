package com.pixipanda.qualitycheck.stat.checkstat

import com.pixipanda.qualitycheck.report.CheckStatReport


abstract class CheckStat {

  def getReportStat: CheckStatReport

  def validate:CheckStat

  def isSuccess: Boolean
}



package com.pixipanda.qualitycheck.stat.checkstat

import com.pixipanda.qualitycheck.report.CheckStatReport


abstract class CheckStat(
var isSuccess: Boolean) {

  def getReportStat:Seq[CheckStatReport]

  def getValidation: String = {
    if(isSuccess) "success" else "failed"
  }
}



package com.pixipanda.qualitycheck.validator

import com.pixipanda.qualitycheck.check.Check
import com.pixipanda.qualitycheck.constant.Checks._
import com.pixipanda.qualitycheck.stat.checkstat.CheckStat


abstract class CheckValidator {

  def validate(stat: CheckStat): Boolean
}

object CheckValidator {

  def getValidator(check: Check): CheckValidator = {
    check.checkType match {
      case NULLCHECK => NullValidator()
      case DISTINCTCHECK => DistinctValidator()
      case UNIQUECHECK => UniqueValidator()
      case ROWCOUNTCHECK => RowCountValidator()
    }
  }
}

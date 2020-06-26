package com.pixipanda.qualitycheck.validator

import com.pixipanda.qualitycheck.stat.checkstat.{CheckStat, UniqueStat}


case class UniqueValidator() extends CheckValidator{

  /*
   This function validates Uniques Stats.
   If there are duplicate records for the give set of columns then returns false else returns true
 */
  def validate(stat: CheckStat): Boolean = {

    val uniqueStatMap = stat.asInstanceOf[UniqueStat].statMap

    val status = uniqueStatMap.forall{
      case (_, count) => count == 0
    }
    if(status) stat.isSuccess = true
    status
  }
}

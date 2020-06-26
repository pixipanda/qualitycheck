package com.pixipanda.qualitycheck.validator


import com.pixipanda.qualitycheck.stat.checkstat.{CheckStat, NullStat}


case class NullValidator() extends CheckValidator{

  def validate(stat: CheckStat): Boolean = {

    val nullStatMap = stat.asInstanceOf[NullStat].statMap

    val status = nullStatMap.forall {
      case (_, count) => count == 0
    }
    if(status) stat.isSuccess = true
    status
  }
}

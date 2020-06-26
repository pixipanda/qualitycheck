package com.pixipanda.qualitycheck.validator

import com.pixipanda.qualitycheck.stat.checkstat.{CheckStat, DistinctStat}


case class DistinctValidator() extends CheckValidator {

  /*
    This function validates Distinct Stats.
    If the computed stats does not match the config then returns false else returns true
   */
  def validate(stat: CheckStat):Boolean = {

    val distinctStatMap = stat.asInstanceOf[DistinctStat].statMap

    val status = distinctStatMap.forall{
      case(distinctConfig, actual) =>
        distinctConfig.relation match {
          case "gt" => actual > distinctConfig.count
          case "ge" => actual >= distinctConfig.count
          case "lt" => actual < distinctConfig.count
          case "le" => actual <= distinctConfig.count
          case "eq" => actual == distinctConfig.count
        }
    }
    if(status) stat.isSuccess = true
    status
  }
}

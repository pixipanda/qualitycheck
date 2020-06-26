package com.pixipanda.qualitycheck.validator


import com.pixipanda.qualitycheck.stat.checkstat.{CheckStat, RowCountStat}

case class RowCountValidator() extends CheckValidator{

  /*
    This function validates RowCount Stats.
    If the computed stats does not match the config then returns false else returns true
   */
  def validate(stat: CheckStat):Boolean = {

    val rowCountStatMap = stat.asInstanceOf[RowCountStat].statMap

    val status = rowCountStatMap.forall{
      case(rowCountConfig, actual) =>
        rowCountConfig.relation match {
          case "gt" => actual > rowCountConfig.count
          case "ge" => actual >= rowCountConfig.count
          case "lt" => actual < rowCountConfig.count
          case "le" => actual <= rowCountConfig.count
          case "eq" => actual == rowCountConfig.count
        }
    }
    if(status) stat.isSuccess = true
    status
  }
}

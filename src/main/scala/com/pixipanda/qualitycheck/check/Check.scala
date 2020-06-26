package com.pixipanda.qualitycheck.check


import com.pixipanda.qualitycheck.stat.checkstat.CheckStat
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.DataFrame



abstract class Check(val checkType: String) {

  def getStat(df: DataFrame):Option[CheckStat]
}

object Check extends LazyLogging {


}

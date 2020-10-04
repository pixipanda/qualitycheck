package com.pixipanda.qualitycheck.check


import com.pixipanda.qualitycheck.stat.checkstat.CheckStat
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ListBuffer



abstract class Check(val checkType: String) {

  def getStat(df: DataFrame):Option[CheckStat]
}

object Check extends LazyLogging {

  def parse(config: Config): Seq[Check] = {

    val checkBuffer = new ListBuffer[Check]

    if(config.hasPath("rowCountCheck")) {
      val rowCountCheck = RowCountCheck.parse(config)
      checkBuffer.append(rowCountCheck)
    }

    if(config.hasPath("nullCheck")) {
      val nullCheck = NullCheck.parse(config)
      checkBuffer.append(nullCheck)
    }

    if(config.hasPath("distinctChecks")) {
      val distinctChecks = DistinctCheck.parse(config)
      checkBuffer.append(distinctChecks)
    }

    if(config.hasPath("uniqueChecks")) {
      val uniqueChecks = UniqueCheck.parse(config)
      checkBuffer.append(uniqueChecks)
    }

    checkBuffer.toList
  }
}

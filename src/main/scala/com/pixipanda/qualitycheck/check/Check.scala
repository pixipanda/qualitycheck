package com.pixipanda.qualitycheck.check


import com.pixipanda.qualitycheck.source.Source
import com.pixipanda.qualitycheck.source.table.JDBC
import com.pixipanda.qualitycheck.stat.checkstat.CheckStat
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ListBuffer



abstract class Check(val checkType: String) {

  val LOGGER: Logger = LoggerFactory.getLogger(getClass.getName)

  def getStat(df: DataFrame):CheckStat

  def getStat(jdbcSource: JDBC): CheckStat

  def getStat(source: Source): CheckStat = {

    if(source.checkOnDF) {
      LOGGER.info("Running quality check on DataFrame")
      getStat(source.getDF)
    } else {
      LOGGER.info("Running quality check using predicate push")
      getStat(source.asInstanceOf[JDBC])
    }
  }
}

object Check {

  val LOGGER: Logger = LoggerFactory.getLogger(getClass.getName)

  def parse(config: Config): Seq[Check] = {

    LOGGER.info("Parsing checks")
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

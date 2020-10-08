package com.pixipanda.qualitycheck.check


import com.pixipanda.qualitycheck.source.Source
import com.pixipanda.qualitycheck.source.table.Table
import com.pixipanda.qualitycheck.stat.checkstat.CheckStat
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ListBuffer



abstract class Check(val checkType: String) {

  def getStat(df: DataFrame):CheckStat

  def predicatePushCount(jdbcOptions: Map[String, String], query: String)(implicit spark: SparkSession):Long = {

    spark.read
      .format("jdbc")
      .options(
        Map(
          "url" -> jdbcOptions("url"),
          "user" -> jdbcOptions("username"),
          "password" -> jdbcOptions("password"),
          "dbtable" -> query,
          "driver" -> jdbcOptions("driver")
        )
      )
      .load()
      .collect()(0).getAs[Long]("count")
  }


  def getStat(source: Source): CheckStat = {

    if(source.checkOnDF) {
      getStat(source.getDF)
    } else {
      getStat(source.asInstanceOf[Table])
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

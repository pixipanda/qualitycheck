package com.pixipanda.qualitycheck.compute

import com.pixipanda.qualitycheck.check.Check
import com.pixipanda.qualitycheck.source.Source
import com.pixipanda.qualitycheck.stat.checkstat.CheckStat
import com.pixipanda.qualitycheck.stat.sourcestat.SourceStat
import org.apache.spark.sql.DataFrame
import org.slf4j.{Logger, LoggerFactory}


object ComputeChecks{

  val LOGGER: Logger = LoggerFactory.getLogger(getClass.getName)

  def runChecks(sources: Seq[Source]): Seq[SourceStat] = {
    sources.map(runChecks)
  }


  def runChecks(source: Source):SourceStat = {

    LOGGER.info(s"Running checks on source: ${source.sourceType}")

    val exists = source.exists
    val fail = false
    if(exists) {
      val checks = source.checks
      val checksStat = checks.map(_.getStat(source).validate)
      val isSuccess = checksStat.forall(_.isSuccess)
      SourceStat(exists, source.getLabel, isSuccess, checksStat)
    }else {
      LOGGER.warn(s"Source: ${source.getLabel} does not exist")
      SourceStat(exists, source.getLabel, fail, Nil)
    }
  }


  def runChecks(checks: Seq[Check], df: DataFrame): Seq[CheckStat] = {
    checks.map(_.getStat(df).validate)
  }
}

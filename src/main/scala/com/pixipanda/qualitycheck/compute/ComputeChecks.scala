package com.pixipanda.qualitycheck.compute

import com.pixipanda.qualitycheck.check.Check
import com.pixipanda.qualitycheck.source.Source
import com.pixipanda.qualitycheck.stat.checkstat.CheckStat
import com.pixipanda.qualitycheck.stat.sourcestat.SourceStat
import com.pixipanda.qualitycheck.validator.CheckValidator
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ListBuffer

case class Result[A](stats: List[A], isSuccess: Boolean)

object ComputeChecks extends LazyLogging{


  def runChecks(sources: Seq[Source]): Result[SourceStat] = {

    object AllDone extends Exception

    val sourceStats = ListBuffer[SourceStat]()

    try {
      sources.foreach (source => {
        val exists = source.exists
        if(exists) {
          val result = runChecks(source.getChecks, source.getDF)
          val sourceStat = SourceStat(exists, source.getLabel, result.isSuccess, result.stats)
          sourceStats.append(sourceStat)
          if (!result.isSuccess) {
            throw AllDone
          }
        }
        else {
          logger.error(s"DataStores ${source.getLabel} does not exist")
          throw AllDone
        }
      })
    } catch {
      case AllDone =>
    }
    Result(sourceStats.toList, sourceStats.last.isSuccess)
  }


  def runChecks(source: Source):Result[CheckStat] = {

    object AllDone extends Exception

    val checkStats = ListBuffer[CheckStat]()

    val df = source.getDF

    val checks = source.getChecks
    val validators = checks.map(CheckValidator.getValidator)
    try {
      checks.indices.foreach (checkIndex => {
        val check = checks(checkIndex)
        val validator = validators(checkIndex)
        val checkStat = check.getStat(df)
        checkStats.append(checkStat.get)
        val isSuccess = validator.validate(checkStat.get)
        if (!isSuccess) {
          throw AllDone
        }
      })
    }
    catch {
      case AllDone =>
    }
    Result(checkStats.toList, checkStats.last.isSuccess)
  }


  def runChecks(checks: Seq[Check], df: DataFrame): Result[CheckStat] = {

    object AllDone extends Exception

    val checkStats = ListBuffer[CheckStat]()

    val validators = checks.map(CheckValidator.getValidator)

    try {
      checks.indices.foreach (checkIndex => {
        val check = checks(checkIndex)
        val validator = validators(checkIndex)
        val checkStat = check.getStat(df)
        checkStats.append(checkStat.get)
        val isSuccess = validator.validate(checkStat.get)
        if (!isSuccess) {
          throw AllDone
        }
      })
    }
    catch {
      case AllDone =>
    }
    Result(checkStats.toList, checkStats.last.isSuccess)
  }
}

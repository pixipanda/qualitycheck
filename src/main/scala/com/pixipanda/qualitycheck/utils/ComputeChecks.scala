package com.pixipanda.qualitycheck.utils

import com.pixipanda.qualitycheck.QualityCheckConfig
import com.pixipanda.qualitycheck.source.Source
import com.pixipanda.qualitycheck.stat.checkstat.CheckStat
import com.pixipanda.qualitycheck.stat.sourcestat.SourceStat
import com.pixipanda.qualitycheck.validator.SourceValidator
import com.typesafe.scalalogging.LazyLogging

import scala.collection.mutable.ListBuffer

case class Result[A](stats: List[A], isSuccess: Boolean)

object ComputeChecks extends LazyLogging{


  def runChecks(qualityCheckConfig: QualityCheckConfig): Result[SourceStat] = {

    object AllDone extends Exception

    val sourceStats = ListBuffer[SourceStat]()
    val sources = qualityCheckConfig.sources
    val sourceValidators = SourceValidator(sources)

    try {
      sources.indices.foreach (sourceIndex => {
        val source = sources(sourceIndex)
        val exists = source.exists
        if(exists) {
          val sourceValidator = sourceValidators(sourceIndex)
          val result = runChecks(source, sourceValidator)
          val sourceStat = SourceStat(exists, source.getLabel, result.isSuccess, result.stats)
          sourceStats.append(sourceStat)
          if (!result.isSuccess) {
            throw AllDone
          }
        }
        else {
          logger.error(s"Source ${source.getLabel} does not exist")
          throw AllDone
        }
      })
    } catch {
      case AllDone =>
    }
    Result(sourceStats.toList, sourceStats.last.isSuccess)
  }


  def runChecks(source: Source, sourceValidator: SourceValidator):Result[CheckStat] = {

    object AllDone extends Exception

    val checkStats = ListBuffer[CheckStat]()

    val df = source.getDF

    val checks = source.getChecks
    val validators = sourceValidator.validators
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

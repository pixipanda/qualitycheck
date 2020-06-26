package com.pixipanda.qualitycheck.utils

import com.pixipanda.qualitycheck.QualityCheckConfig
import com.pixipanda.qualitycheck.check._
import com.pixipanda.qualitycheck.constant.Checks._
import com.pixipanda.qualitycheck.report.ReportBuilder
import com.pixipanda.qualitycheck.source.{DataFrameTestFailure, DataFrameTestSuccess}
import com.pixipanda.qualitycheck.stat.checkstat._
import com.pixipanda.qualitycheck.stat.sourcestat.SourceStat
import com.pixipanda.qualitycheck.validator.SourceValidator
import org.scalatest.FunSpec

class ComputeChecksSpec extends FunSpec {

  describe("ComputeChecks") {

    describe("Functionality") {

      val successConfig = QualityCheckConfig(
        List(
          DataFrameTestSuccess(
            "testSpark",
            "testDb",
            "testTable",
            null,
            List(
              RowCountCheck(0, "gt", ROWCOUNTCHECK),
              NullCheck(List("quantity"), NULLCHECK),
              DistinctCheck(List(DistinctRelation(List("item"), 2, "ge")), DISTINCTCHECK),
              UniqueCheck(List(List("item"), List("price"), List("quantity")), UNIQUECHECK)
            )
          )
        )
      )


      val failureConfig = QualityCheckConfig(
        List(
          DataFrameTestFailure(
            "testSpark",
            "testDb",
            "testTable",
            "testquery",
            List(
              RowCountCheck(0, "gt", ROWCOUNTCHECK),
              NullCheck(List("quantity"), NULLCHECK),
              DistinctCheck(List(DistinctRelation(List("item"), 2, "ge")), DISTINCTCHECK),
              UniqueCheck(List(List("item"), List("price"), List("quantity")), UNIQUECHECK)
            )
          )
        )
      )

      val exists = true

   /*   val totalRowCountStatMap = Map(RowCountCheck(0, "gt", TOTALROWCOUTNCHECK) -> 4L)
      val totalRowCountStat = RowCountStat(totalRowCountStatMap)

*/
      val rowCountStatMap = Map(RowCountCheck(0, "gt", ROWCOUNTCHECK) -> 4L)
      val rowCountStat = RowCountStat(rowCountStatMap)


      val nullStatMap = Map("quantity" -> 0L)
      val nullStat = NullStat(nullStatMap)


      val distinctStatMap = Map(DistinctRelation(List("item"), 2, "ge") -> 4L)
      val distinctStat = DistinctStat(distinctStatMap)


      val uniqueStatMap = Map("item" -> 0L, "price" -> 0L, "quantity" -> 0L)
      val uniqueStat = UniqueStat(uniqueStatMap)


      val stats:List[CheckStat] = List(
        rowCountStat,
        nullStat,
        distinctStat,
        uniqueStat
      )

      it("checkStats success") {
        val isSuccess = true
        val expectedResult = Result(stats, isSuccess)
        val sources = successConfig.sources
        val sourceValidators = SourceValidator(sources)

        sources.indices.foreach(sourceIndex => {
          val source = sources(sourceIndex)
          val sourceValidator = sourceValidators(sourceIndex)
          val sut = ComputeChecks.runChecks(source, sourceValidator)

          assert(sut == expectedResult)
          sut.stats.foreach(stat => {
            assert(stat.isSuccess)
          })
        })
      }

      it("checkStats null check failure") {
        val isSuccess = false
        val nullStatMap = Map("quantity" -> 1L)
        val nullStat = NullStat(nullStatMap)
        val expectedResult = Result[CheckStat](List(rowCountStat, nullStat), isSuccess)

        val sources = failureConfig.sources
        val sourceValidators = SourceValidator(sources)

        sources.indices.foreach(sourceIndex => {
          val source = sources(sourceIndex)
          val sourceValidator = sourceValidators(sourceIndex)
          val sut = ComputeChecks.runChecks(source, sourceValidator)

          assert(sut == expectedResult)
          val lastStat = sut.stats.last
          assert(!lastStat.isSuccess)
          val otherStats = sut.stats.init
          otherStats.foreach(stat => {
             assert(stat.isSuccess)
          })
        })
      }

      val isSuccess = true
      val sourceStat = SourceStat(exists, "testDb_testTable", isSuccess, stats)
      val expectedResult = Result(List(sourceStat), isSuccess)

      it("sourceStats success") {
        val sut = ComputeChecks.runChecks(successConfig)
        assert(sut == expectedResult)
      }

      it("Success Report") {
        val isSuccess = true
        val result = ComputeChecks.runChecks(successConfig)
        val report = ReportBuilder.buildReport(result.stats)
        report.show(false)
      }

      it("Failure Report") {
        val isSuccess = false
        val result = ComputeChecks.runChecks(failureConfig)
        val report = ReportBuilder.buildReport(result.stats)
        report.show(false)
      }


    }
  }
}

package com.pixipanda.qualitycheck.compute

import com.pixipanda.qualitycheck.TestConfig
import com.pixipanda.qualitycheck.check._
import com.pixipanda.qualitycheck.constant.Checks._
import com.pixipanda.qualitycheck.stat.checkstat._
import com.pixipanda.qualitycheck.stat.sourcestat.SourceStat
import org.scalatest.FunSpec

class ComputeChecksSpec extends FunSpec {

  describe("ComputeChecks") {

    describe("Functionality") {

      val exists = true
      val isSuccess = true

      val rowCountStatMap = Map(RowCountCheck(0, "gt", ROWCOUNTCHECK) -> (4L,true))
      val rowCountStat = RowCountStat(rowCountStatMap)

      val successNullStatMap = Map("quantity" -> (0L, true))
      val nullStat = NullStat(successNullStatMap)

      val distinctStatMap = Map(DistinctRelation(List("item"), 2, "ge") -> (4L, true))
      val distinctStat = DistinctStat(distinctStatMap)


      val uniqueStatMap = Map("item" -> (0L, true), "price" -> (0L, true), "quantity" -> (0L, true))
      val uniqueStat = UniqueStat(uniqueStatMap)


      val stats = List(rowCountStat, nullStat, distinctStat, uniqueStat)

      it("should run checks and all the checks should succeed") {

        val expectedResult = SourceStat(exists, "testSpark:testDb:testTable", isSuccess, stats)
        val sources = TestConfig.successConfig.sources

        sources.foreach(source => {
          val sut = ComputeChecks.runChecks(source)
          assert(sut == expectedResult)
          sut.stats.foreach(stat => {
            assert(stat.isSuccess)
          })
        })
      }

      it("should run checks and null check should fail") {
        val isSuccess = false
        val failureNullStatMap = Map("quantity" -> (1L, false))
        val nullStat = NullStat(failureNullStatMap)
        val expectedResult = List(SourceStat(exists, "testSpark:testDb:testTable:testquery", isSuccess, List(rowCountStat, nullStat, distinctStat, uniqueStat)))

        val sources = TestConfig.failureConfig.sources

        val sut = ComputeChecks.runChecks(sources)
        assert(sut == expectedResult)
        val sut1 = sut.forall(_.isSuccess)
        assert(!sut1)
      }


      it("should run all checks and return success source stat ") {
        val expectedResult = List(SourceStat(exists, "testSpark:testDb:testTable", isSuccess, stats))
        val sut = ComputeChecks.runChecks(TestConfig.successConfig.sources)
        assert(sut == expectedResult)
      }
    }
  }
}

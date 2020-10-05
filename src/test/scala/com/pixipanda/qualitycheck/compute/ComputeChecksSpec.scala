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

      val rowCountStatMap = Map(RowCountCheck(0, "gt", ROWCOUNTCHECK) -> 4L)
      val rowCountStat = RowCountStat(rowCountStatMap, isSuccess)


      val nullStatMap = Map("quantity" -> 0L)
      val nullStat = NullStat(nullStatMap, isSuccess)


      val distinctStatMap = Map(DistinctRelation(List("item"), 2, "ge") -> 4L)
      val distinctStat = DistinctStat(distinctStatMap, isSuccess)


      val uniqueStatMap = Map("item" -> 0L, "price" -> 0L, "quantity" -> 0L)
      val uniqueStat = UniqueStat(uniqueStatMap, isSuccess)


      val stats:List[CheckStat] = List(
        rowCountStat,
        nullStat,
        distinctStat,
        uniqueStat
      )

      it("should run checks and should return success for each stats") {
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

      it("should run null check and should return failure status") {
        val isSuccess = false
        val nullStatMap = Map("quantity" -> 1L)
        val nullStat = NullStat(nullStatMap, isSuccess)
        val expectedResult = List(SourceStat(exists, "testSpark:testDb:testTable:testquery", isSuccess, List(rowCountStat, nullStat, distinctStat, uniqueStat)))

        val sources = TestConfig.failureConfig.sources

        val sut = ComputeChecks.runChecks(sources)
        assert(sut == expectedResult)
        val sut1 = sut.forall(_.isSuccess)
        assert(!sut1)
      }


      it("should run checks and should return success sourceStats") {
        val expectedResult = List(SourceStat(exists, "testSpark:testDb:testTable", isSuccess, stats))
        val sut = ComputeChecks.runChecks(TestConfig.successConfig.sources)
        assert(sut == expectedResult)
      }
    }
  }
}

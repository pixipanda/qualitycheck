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
        val sources = TestConfig.successConfig.sources

        sources.foreach(source => {
          val sut = ComputeChecks.runChecks(source)
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

        val sources = TestConfig.failureConfig.sources

        sources.foreach(source => {
          val sut = ComputeChecks.runChecks(source)
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
      val sourceStat = SourceStat(exists, "testSpark:testDb:testTable", isSuccess, stats)
      val expectedResult = Result(List(sourceStat), isSuccess)

      it("sourceStats success") {
        val sut = ComputeChecks.runChecks(TestConfig.successConfig.sources)
        assert(sut == expectedResult)
      }
    }
  }
}

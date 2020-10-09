package com.pixipanda.qualitycheck.stat

import com.pixipanda.qualitycheck.check.DistinctRelation
import com.pixipanda.qualitycheck.stat.checkstat.DistinctStat
import org.scalatest.FunSpec

class DistinctStatSpec extends  FunSpec{

  describe("DistinctStatSpec") {
    describe("DistinctStat Functionality") {

      it("should validate distinct stat and return success") {
        val distinctStatMap = Map(DistinctRelation(List("item"), 2, "ge") -> (4L,true))
        val distinctStat = DistinctStat(distinctStatMap)
        val sut = distinctStat.validate.isSuccess
        assert(sut)
      }

      it("should validate distinct stat and return failure") {
        val distinctStatMap = Map(DistinctRelation(List("item"), 2, "ge") -> (1L,false))
        val distinctStat = DistinctStat(distinctStatMap)
        val sut = distinctStat.validate.isSuccess
        assert(!sut)
      }
    }
  }
}

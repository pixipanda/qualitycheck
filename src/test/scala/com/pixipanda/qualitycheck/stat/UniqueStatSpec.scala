package com.pixipanda.qualitycheck.stat

import com.pixipanda.qualitycheck.stat.checkstat.UniqueStat
import org.scalatest.FunSpec

class UniqueStatSpec extends FunSpec{

  describe("UniqueStatSpec") {

    describe("UniqueStat Functionality") {

      it("should validate unique stat and return success") {
        val uniqueStatMap = Map("item" -> (0L,true), "price" -> (0L,true), "quantity" -> (0L,true))
        val uniqueStat = UniqueStat(uniqueStatMap)
        val sut = uniqueStat.validate
        assert(sut.isSuccess)
      }

      it("should validate unique stat and return failure") {
        val uniqueStatMap = Map("item" -> (1L, false), "price" -> (0L,true), "quantity" -> (0L, true))
        val uniqueStat = UniqueStat(uniqueStatMap)
        val sut = uniqueStat.validate
        assert(!sut.isSuccess)
      }
    }
  }
}

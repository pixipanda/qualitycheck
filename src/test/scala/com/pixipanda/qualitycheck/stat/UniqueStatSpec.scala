package com.pixipanda.qualitycheck.stat

import com.pixipanda.qualitycheck.stat.checkstat.UniqueStat
import org.scalatest.FunSpec

class UniqueStatSpec extends FunSpec{

  describe("UniqueStatSpec") {

    describe("UniqueStat Functionality") {

      it("success validator") {
        val uniqueStatMap = Map("item" -> 0L, "price" -> 0L, "quantity" -> 0L)
        val uniqueStat = UniqueStat(uniqueStatMap)
        val sut = uniqueStat.validate
        assert(sut.isSuccess)
      }

      it("failure validator") {
        val uniqueStatMap = Map("item" -> 1L, "price" -> 0L, "quantity" -> 0L)
        val uniqueStat = UniqueStat(uniqueStatMap)
        val sut = uniqueStat.validate
        assert(!sut.isSuccess)
      }
    }
  }
}

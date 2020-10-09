package com.pixipanda.qualitycheck.stat

import com.pixipanda.qualitycheck.stat.checkstat.NullStat
import org.scalatest.FunSpec

class NullStatSpec extends  FunSpec{

  describe("NullStatSpec") {

    describe("NullStat Functionality") {

      it("should validate null stat and return success") {
        val nullStatMap = Map("quantity" -> (0L, true))
        val nullStat = NullStat(nullStatMap)
        val sut = nullStat.validate
        assert(sut.isSuccess)
      }

      it("should validate null stat and return failure") {
        val nullStatMap = Map("quantity" -> (10L, false))
        val nullStat = NullStat(nullStatMap)
        val sut = nullStat.validate
        assert(!sut.isSuccess)
      }
    }
  }
}

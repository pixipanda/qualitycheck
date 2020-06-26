package com.pixipanda.qualitycheck.validator

import com.pixipanda.qualitycheck.check.NullCheck
import com.pixipanda.qualitycheck.constant.Checks._
import com.pixipanda.qualitycheck.stat.checkstat.NullStat
import org.scalatest.FunSpec

class NullValidatorSpec extends FunSpec{

  describe("NullValidator") {

    describe("NullValidator Functionality") {

      val nullCheck =NullCheck(List("quantity"), NULLCHECK)
      it("success validator") {
        val nullStatMap = Map("quantity" -> 0L)
        val nullStat = NullStat(nullStatMap)
        val nullValidator = CheckValidator.getValidator(nullCheck)
        val sut = nullValidator.validate(nullStat)
        assert(sut)
        assert(nullStat.isSuccess)
      }

      it("failure validator") {
        val nullStatMap = Map("quantity" -> 10L)
        val nullStat = NullStat(nullStatMap)
        val nullValidator = CheckValidator.getValidator(nullCheck)
        val sut = nullValidator.validate(nullStat)
        assert(!sut)
        assert(!nullStat.isSuccess)
      }
    }
  }
}

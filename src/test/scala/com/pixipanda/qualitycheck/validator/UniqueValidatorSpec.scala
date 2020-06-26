package com.pixipanda.qualitycheck.validator

import com.pixipanda.qualitycheck.check.UniqueCheck
import com.pixipanda.qualitycheck.constant.Checks.UNIQUECHECK
import com.pixipanda.qualitycheck.stat.checkstat.UniqueStat
import org.scalatest.FunSpec

class UniqueValidatorSpec extends FunSpec{

  describe("UniqueValidator") {

    describe("UniqueValidator Functionality") {

      val uniqueCheck = UniqueCheck(List(List("item"), List("price"), List("quantity")), UNIQUECHECK)
      it("success validator") {
        val uniqueStatMap = Map("item" -> 0L, "price" -> 0L, "quantity" -> 0L)
        val uniqueStat = UniqueStat(uniqueStatMap)
        val uniqueValidator = CheckValidator.getValidator(uniqueCheck)
        val sut = uniqueValidator.validate(uniqueStat)
        assert(sut)
        assert(uniqueStat.isSuccess)
      }

      it("failure validator") {
        val uniqueStatMap = Map("item" -> 1L, "price" -> 0L, "quantity" -> 0L)
        val uniqueStat = UniqueStat(uniqueStatMap)
        val uniqueValidator = CheckValidator.getValidator(uniqueCheck)
        val sut = uniqueValidator.validate(uniqueStat)
        assert(!sut)
        assert(!uniqueStat.isSuccess)
      }
    }
  }
}

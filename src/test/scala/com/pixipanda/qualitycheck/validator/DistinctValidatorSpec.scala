package com.pixipanda.qualitycheck.validator

import com.pixipanda.qualitycheck.check.{DistinctCheck, DistinctRelation}
import com.pixipanda.qualitycheck.constant.Checks.DISTINCTCHECK
import com.pixipanda.qualitycheck.stat.checkstat.DistinctStat
import org.scalatest.FunSpec

class DistinctValidatorSpec extends FunSpec{

  describe("DistinctValidator") {

    describe("DistinctValidator Functionality") {
      val distinctCheck = DistinctCheck(List(DistinctRelation(List("item"), 2, "ge")), DISTINCTCHECK)
      it("success ge validator") {
        val distinctStatMap = Map(DistinctRelation(List("item"), 2, "ge") -> 4L)
        val distinctStat = DistinctStat(distinctStatMap)
        val distinctValidator = CheckValidator.getValidator(distinctCheck)
        val sut = distinctValidator.validate(distinctStat)
        assert(sut)
        assert(distinctStat.isSuccess)
      }

      it("failure ge validator") {
        val distinctStatMap = Map(DistinctRelation(List("item"), 2, "ge") -> 1L)
        val distinctStat = DistinctStat(distinctStatMap)
        val distinctValidator = CheckValidator.getValidator(distinctCheck)
        val sut = distinctValidator.validate(distinctStat)
        assert(!sut)
        assert(!distinctStat.isSuccess)
      }
    }
  }
}

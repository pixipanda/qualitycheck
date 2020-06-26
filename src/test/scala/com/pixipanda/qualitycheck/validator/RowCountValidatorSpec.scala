package com.pixipanda.qualitycheck.validator

import com.pixipanda.qualitycheck.check.RowCountCheck
import com.pixipanda.qualitycheck.constant.Checks._
import com.pixipanda.qualitycheck.stat.checkstat.RowCountStat
import org.scalatest.FunSpec

class RowCountValidatorSpec extends FunSpec{

 /* describe("TotalRowCountValidator") {

    describe("TotalRowCountValidator Functionality") {

      val rowCountCheck = RowCountCheck(0, "gt", ROWCOUNTCHECK)
      it("success validator") {

        val rowCountStatMap = Map(rowCountCheck -> 4L)
        val rowCountStat = RowCountStat(rowCountStatMap)
        val rowCountValidator = CheckValidator.getValidator(rowCountCheck)
        val sut = rowCountValidator.validate(rowCountStat)
        assert(sut)
        assert(rowCountStat.isSuccess)
      }

      it("failure validator") {
        val rowCountStatMap = Map(rowCountCheck -> 0L)
        val rowCountStat = RowCountStat(rowCountStatMap)
        val rowCountValidator = CheckValidator.getValidator(rowCountCheck)
        val sut = rowCountValidator.validate(rowCountStat)
        assert(!sut)
        assert(!rowCountStat.isSuccess)
      }
    }
  }*/

  describe("RowCountValidator") {

    describe("RowCountValidator Functionality") {

      val rowCountCheck = RowCountCheck(0, "gt", ROWCOUNTCHECK)
      it("success validator") {
        val rowCountStatMap = Map(rowCountCheck -> 4L)
        val rowCountStat = RowCountStat(rowCountStatMap)
        val rowCountValidator = CheckValidator.getValidator(rowCountCheck)
        val sut = rowCountValidator.validate(rowCountStat)
        assert(sut)
        assert(rowCountStat.isSuccess)
      }

      it("failure validator") {
        val rowCountStatMap = Map(rowCountCheck -> 0L)
        val rowCountStat = RowCountStat(rowCountStatMap)
        val rowCountValidator = CheckValidator.getValidator(rowCountCheck)
        val sut = rowCountValidator.validate(rowCountStat)
        assert(!sut)
        assert(!rowCountStat.isSuccess)
      }
    }
  }
}

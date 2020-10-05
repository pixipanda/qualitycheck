package com.pixipanda.qualitycheck.stat

import com.pixipanda.qualitycheck.check.RowCountCheck
import com.pixipanda.qualitycheck.constant.Checks.ROWCOUNTCHECK
import com.pixipanda.qualitycheck.stat.checkstat.RowCountStat
import org.scalatest.FunSpec

class RowCountStatSpec extends  FunSpec{

  describe("RowCountStatSpec") {

    describe("RowCountStat Functionality") {

      val rowCountCheck = RowCountCheck(0, "gt", ROWCOUNTCHECK)
      it("should validate row count stat and return success") {
        val rowCountStatMap = Map(rowCountCheck -> 4L)
        val rowCountStat = RowCountStat(rowCountStatMap)
        val sut = rowCountStat.validate
        assert(sut.isSuccess)
      }

      it("should validate row count stat and return failure") {
        val rowCountStatMap = Map(rowCountCheck -> 0L)
        val rowCountStat = RowCountStat(rowCountStatMap)
        val sut = rowCountStat.validate
        assert(!sut.isSuccess)
      }
    }
  }
}

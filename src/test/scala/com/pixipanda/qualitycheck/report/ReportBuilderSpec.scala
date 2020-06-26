package com.pixipanda.qualitycheck.report

import com.pixipanda.qualitycheck.TestConfig
import com.pixipanda.qualitycheck.utils.ComputeChecks
import org.scalatest.FunSpec

class ReportBuilderSpec extends FunSpec{

  describe("ReportBuilder") {

    describe("Functionality") {

      it("Success Report") {
        val result = ComputeChecks.runChecks(TestConfig.successConfig)
        val report = ReportBuilder.buildReport(result.stats)
        report.show(false)
      }

      it("Failure Report") {
        val result = ComputeChecks.runChecks(TestConfig.failureConfig)
        val report = ReportBuilder.buildReport(result.stats)
        report.show(false)
      }
    }
  }
}

package com.pixipanda.qualitycheck.report

import com.pixipanda.qualitycheck.TestConfig
import com.pixipanda.qualitycheck.compute.ComputeChecks
import org.scalatest.FunSpec

class ReportBuilderSpec extends FunSpec{

  describe("ReportBuilder") {

    describe("Functionality") {

      it("Success Report") {
        val sourcesStat = ComputeChecks.runChecks(TestConfig.successConfig.sources)
        val report = ReportBuilder.buildReportDF(sourcesStat)
        report.show(false)
      }

      it("Failure Report") {
        val sourcesStat = ComputeChecks.runChecks(TestConfig.failureConfig.sources)
        val report = ReportBuilder.buildReportDF(sourcesStat)
        report.show(false)
      }
    }
  }
}

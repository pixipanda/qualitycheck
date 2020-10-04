package com.pixipanda.qualitycheck

import com.pixipanda.qualitycheck.compute.ComputeChecks
import com.pixipanda.qualitycheck.report.ReportBuilder
import org.scalatest.FunSpec

class StatApiSpec extends FunSpec{

  describe("StatApi") {

    describe("Functionality") {

      it("Success Stat Json") {
        val result = ComputeChecks.runChecks(TestConfig.successConfig.sources)
        val report = ReportBuilder.buildReport(result.stats)
        report.coalesce(1).write.option("header", "true").json("/tmp/json_success")
      }

      it("Success Stat Csv") {
        val result = ComputeChecks.runChecks(TestConfig.successConfig.sources)
        val report = ReportBuilder.buildReport(result.stats)
        report.coalesce(1).write.option("header", "true").csv("/tmp/csv_success")
      }

      it("Failure Report Json") {
        val result = ComputeChecks.runChecks(TestConfig.failureConfig.sources)
        val report = ReportBuilder.buildReport(result.stats)
        report.coalesce(1).write.option("header", "true").json("/tmp/json_failure")
      }

      it("Failure Report Csv") {
        val result = ComputeChecks.runChecks(TestConfig.failureConfig.sources)
        val report = ReportBuilder.buildReport(result.stats)
        report.coalesce(1).write.option("header", "true").json("/tmp/csv_failure")
      }
    }
  }
}

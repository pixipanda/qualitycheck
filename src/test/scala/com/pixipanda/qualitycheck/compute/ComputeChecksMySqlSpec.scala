package com.pixipanda.qualitycheck.compute

import com.pixipanda.qualitycheck.config.ConfigParser
import com.pixipanda.qualitycheck.report.ReportBuilder
import org.scalatest.FunSpec

class ComputeChecksMySqlSpec extends  FunSpec{

  describe("ComputeChecksMySqlSpec") {

    describe("Functionality Test") {

      it("should compute row count check on mysql database using mysql.conf") {
        val mySqlQualityCheckConfig = ConfigParser.parseQualityCheck("src/test/resources/qualityCheck/mysql.conf")
        val mySqlStat = ComputeChecks.runChecks(mySqlQualityCheckConfig.sources)
        val mySqlReportDF = ReportBuilder.buildReportDF(mySqlStat)
        mySqlReportDF.show(false)
      }
    }
  }
}

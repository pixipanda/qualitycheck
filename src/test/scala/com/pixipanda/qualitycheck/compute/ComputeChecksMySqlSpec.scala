package com.pixipanda.qualitycheck.compute

import com.pixipanda.qualitycheck.config.ConfigParser
import com.pixipanda.qualitycheck.report.ReportBuilder
import org.scalatest.FunSpec

class ComputeChecksMySqlSpec extends  FunSpec{

  describe("ComputeChecksMySqlSpec") {

    describe("Functionality Test") {

      it("should compute row count check on mysql database using mysqlRowCount.conf") {
        val mySqlQualityCheckConfig = ConfigParser.parseQualityCheck("src/test/resources/qualityCheck/mysql/mysqlRowCount.conf")
        val mySqlStat = ComputeChecks.runChecks(mySqlQualityCheckConfig.sources)
        val mySqlReportDF = ReportBuilder.buildReportDF(mySqlStat)
        mySqlReportDF.show(false)
      }

      it("should compute distinct check on mysql database using mysqlDistinctCheck.conf") {
        val mySqlQualityCheckConfig = ConfigParser.parseQualityCheck("src/test/resources/qualityCheck/mysql/mysqlDistinctCheck.conf")
        val mySqlStat = ComputeChecks.runChecks(mySqlQualityCheckConfig.sources)
        val mySqlReportDF = ReportBuilder.buildReportDF(mySqlStat)
        mySqlReportDF.show(false)
      }

      it("should compute distinct check on mysql database using mysqlNullCheck.conf") {
        val mySqlQualityCheckConfig = ConfigParser.parseQualityCheck("src/test/resources/qualityCheck/mysql/mysqlNullCheck.conf")
        val mySqlStat = ComputeChecks.runChecks(mySqlQualityCheckConfig.sources)
        val mySqlReportDF = ReportBuilder.buildReportDF(mySqlStat)
        mySqlReportDF.show(false)
      }

      it("should compute distinct check on mysql database using mysqlUniqueCheck.conf") {
        val mySqlQualityCheckConfig = ConfigParser.parseQualityCheck("src/test/resources/qualityCheck/mysql/mysqlUniqueCheck.conf")
        val mySqlStat = ComputeChecks.runChecks(mySqlQualityCheckConfig.sources)
        val mySqlReportDF = ReportBuilder.buildReportDF(mySqlStat)
        mySqlReportDF.show(false)
      }
    }
  }
}

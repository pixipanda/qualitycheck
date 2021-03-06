package com.pixipanda.qualitycheck.check

import com.pixipanda.qualitycheck.{QualityCheckConfig, TestingSparkSession}
import com.pixipanda.qualitycheck.TestHelpers._
import com.pixipanda.qualitycheck.constant.Checks.ROWCOUNTCHECK
import com.pixipanda.qualitycheck.source.table.Hive
import com.pixipanda.qualitycheck.stat.checkstat.RowCountStat
import com.typesafe.config.ConfigFactory
import org.scalatest.FunSpec

class RowCountCheckSpec extends FunSpec with TestingSparkSession{

  describe("QueryRowCountCheck") {

    describe("config parsing") {

      it("should parse row count check config string") {
        val rowCountCheckString =
          """
            | rowCountCheck {
            |    count = 0,
            |    relation = "gt"
            | }
          """.stripMargin

        val config = ConfigFactory.parseString(rowCountCheckString)
        val sut = RowCountCheck.parse(config)
        assert(sut ==  RowCountCheck(0, "gt", ROWCOUNTCHECK))
      }
    }

    describe("functionality") {
      val predicatePush = false
      val config = QualityCheckConfig(
        List(
          Hive(
            "hive",
            "db1",
            "table1",
            "query1",
            predicatePush,
            List(
              RowCountCheck(0, "gt", ROWCOUNTCHECK)
            )
          )
        )
      )

      it("should compute row count stat with rowCount greater than zero") {
        val itemMap = Map(
          "item"     -> List("Eggs", "Milk", "Bread", "Cheese"),
          "price"    -> List(  5.49,   3.89,    4.50,     6.00),
          "quantity" -> List(    12,      5,       2,       10)
        )
        val dF= mkDF(spark, itemMap.toSeq: _*)
        val rowCountCheck = RowCountCheck(0, "gt", ROWCOUNTCHECK)
        val rowCountStatMap = Map(rowCountCheck -> (4L,false))
        val rowCountStat = RowCountStat(rowCountStatMap)
        CheckTestHelper.testStat(config, rowCountStat, dF)
      }
    }
  }
}

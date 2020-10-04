package com.pixipanda.qualitycheck.check

import com.pixipanda.qualitycheck.{QualityCheckConfig, TestingSparkSession}
import com.pixipanda.qualitycheck.TestHelpers.mkDF
import com.pixipanda.qualitycheck.constant.Checks.UNIQUECHECK
import com.pixipanda.qualitycheck.source.table.Hive
import com.pixipanda.qualitycheck.stat.checkstat.UniqueStat
import com.typesafe.config.ConfigFactory
import org.scalatest.FunSpec


class UniqueCheckSpec extends FunSpec with TestingSparkSession{

  describe("UniqueCheck") {

    describe("config parsing") {

      it("basic config parsing") {
        val uniqueCheckString =
          """
            |uniqueChecks = [
            |  ["item"],
            |  ["price"]
            |  ["quantity"]
            |]
          """.stripMargin

        val config = ConfigFactory.parseString(uniqueCheckString)
        val sut = UniqueCheck.parse(config)
        assert(sut ==  UniqueCheck(List(List("item"), List("price"), List("quantity")), UNIQUECHECK))
      }
    }

    describe("functionality") {
      val config = QualityCheckConfig(
        List(
          Hive(
            "hive",
            "db1",
            "table1",
            "query1",
            List(
              UniqueCheck(List(List("item"), List("price"), List("quantity")), UNIQUECHECK)
            )
          )
        )
      )

      it("uniqueCheck success") {
        val itemMap = Map(
          "item"     -> List("Eggs", "Milk", "Bread", "Cheese"),
          "price"    -> List(  5.49,   3.89,    4.50,     6.00),
          "quantity" -> List(    12,      5,       2,       10)
        )
        val dF= mkDF(spark, itemMap.toSeq: _*)
        val uniqueStatMap = Map("item" -> 0L, "price" -> 0L, "quantity" -> 0L)
        val uniqueStat = UniqueStat(uniqueStatMap)
        CheckTestHelper.testStat(config, uniqueStat, dF)
      }
    }
  }
}

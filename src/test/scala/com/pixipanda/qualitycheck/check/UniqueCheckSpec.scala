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

      it("should parse unique check config string") {
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
      val checkOnDF = true
      val config = QualityCheckConfig(
        List(
          Hive(
            "hive",
            "db1",
            "table1",
            "query1",
            checkOnDF,
            List(
              UniqueCheck(List(List("item"), List("price"), List("quantity")), UNIQUECHECK)
            )
          )
        )
      )

      it("should give uniqueCheck stat") {
        val itemMap = Map(
          "item"     -> List("Eggs", "Milk", "Bread", "Cheese"),
          "price"    -> List(  5.49,   3.89,    4.50,     6.00),
          "quantity" -> List(    12,      5,       2,       10)
        )
        val dF= mkDF(spark, itemMap.toSeq: _*)
        val uniqueStatMap = Map("item" -> (0L,false), "price" -> (0L,false), "quantity" -> (0L,false))
        val uniqueStat = UniqueStat(uniqueStatMap)
        CheckTestHelper.testStat(config, uniqueStat, dF)
      }
    }
  }
}

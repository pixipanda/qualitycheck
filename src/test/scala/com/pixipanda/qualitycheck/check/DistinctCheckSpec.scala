package com.pixipanda.qualitycheck.check

import com.pixipanda.qualitycheck.{QualityCheckConfig, TestingSparkSession}
import com.pixipanda.qualitycheck.TestHelpers._
import com.pixipanda.qualitycheck.constant.Checks.DISTINCTCHECK
import com.pixipanda.qualitycheck.source.table.Hive
import com.pixipanda.qualitycheck.stat.checkstat.DistinctStat
import com.typesafe.config.ConfigFactory
import org.scalatest.FunSpec

class DistinctCheckSpec extends FunSpec with TestingSparkSession{

  describe("DistinctCheck") {

    describe("config parsing") {

      it("should parse distinct checks config string") {
        val distinctCheckString =
          """
            | distinctChecks = [
            |        {columns = ["item"], count = 2, relation = "ge"},
            |        {columns = ["price"], count = 1, relation = "ge"},
            |        {columns = ["quantity"], count = 1, relation = "ge"}
            |]
          """.stripMargin
        val drItem = DistinctRelation(List("item"), 2, "ge")
        val drPrice = DistinctRelation(List("price"), 1, "ge")
        val drQuantity = DistinctRelation(List("quantity"), 1, "ge")
        val config = ConfigFactory.parseString(distinctCheckString)
        val sut = DistinctCheck.parse(config)
        assert(sut ==  DistinctCheck(List(drItem, drPrice, drQuantity), DISTINCTCHECK))
      }
    }

    describe("functionality") {
      val checkOnDF = true
      val qualityCheckConfig = QualityCheckConfig(
        List(
          Hive(
            "hive",
            "db1",
            "table1",
            "query1",
            checkOnDF,
            List(
              DistinctCheck(List(DistinctRelation(List("item"), 2, "ge")), DISTINCTCHECK)
            )
          )
        )
      )

      it("should do distinct check and return distinct stat") {
        val itemMap = Map(
          "item"     -> List("Eggs", "Milk", "Bread", "Cheese"),
          "price"    -> List(  5.49,   3.89,    4.50,     6.00),
          "quantity" -> List(    12,      5,       2,       10)
        )
        val dF= mkDF(spark, itemMap.toSeq: _*)
        val distinctStatMap = Map(DistinctRelation(List("item"), 2, "ge") -> (4L, false))
        val distinctStat = DistinctStat(distinctStatMap)
        CheckTestHelper.testStat(qualityCheckConfig, distinctStat, dF)
      }
    }
  }
}

package com.pixipanda.qualitycheck.check

import cats.syntax.either._
import com.lowes.TestingSparkSession
import com.pixipanda.qualitycheck.QualityCheckConfig
import com.pixipanda.qualitycheck.TestHelpers._
import com.pixipanda.qualitycheck.constant.Checks.DISTINCTCHECK
import com.pixipanda.qualitycheck.source.Hive
import com.pixipanda.qualitycheck.stat.checkstat.DistinctStat
import io.circe.{Json, parser}
import io.circe.config.{parser => configParser}
import org.scalatest.FunSpec

class DistinctCheckSpec extends FunSpec with TestingSparkSession{

  describe("DistinctCheck") {

    describe("config parsing") {

      it("basic config parsing") {
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
        val doc = configParser.parse(distinctCheckString).getOrElse(Json.Null)
        val distinctCheckJson = doc.hcursor.downField("distinctChecks").focus.get
        val sut = parser.decode[List[DistinctRelation]](distinctCheckJson.toString)
        assert(sut ==  Right(List(drItem, drPrice,drQuantity)))
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
              DistinctCheck(List(DistinctRelation(List("item"), 2, "ge")), DISTINCTCHECK)
            )
          )
        )
      )

      it("distinct success") {
        val itemMap = Map(
          "item"     -> List("Eggs", "Milk", "Bread", "Cheese"),
          "price"    -> List(  5.49,   3.89,    4.50,     6.00),
          "quantity" -> List(    12,      5,       2,       10)
        )
        val dF= mkDF(spark, itemMap.toSeq: _*)
        val distinctStatMap = Map(DistinctRelation(List("item"), 2, "ge") -> 4L)
        val distinctStat = DistinctStat(distinctStatMap)
        CheckTestHelper.testStat(config, distinctStat, dF)
      }
    }
  }
}

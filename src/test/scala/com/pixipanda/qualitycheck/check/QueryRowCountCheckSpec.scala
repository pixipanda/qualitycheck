/*
package com.pixipanda.qualitycheck.check

import cats.syntax.either._
import com.pixipanda.qualitycheck.TestingSparkSession
import com.pixipanda.qualitycheck.QualityCheckConfig
import com.pixipanda.qualitycheck.TestHelpers._
import com.pixipanda.qualitycheck.constant.Checks.QUERYROWCOUTNCHECK
import com.pixipanda.qualitycheck.source.Hive
import com.pixipanda.qualitycheck.stat.checkstat.RowCountStat
import io.circe.config.{parser => configParser}
import io.circe.{Json, parser}
import org.scalatest.FunSpec

class QueryRowCountCheckSpec extends FunSpec with TestingSparkSession{

  describe("QueryRowCountCheck") {

    describe("config parsing") {

      it("basic config parsing") {
        val queryRowCountCheckString =
          """
            | queryRowCountCheck {
            |    count = 0,
            |    relation = "gt"
            | }
          """.stripMargin

        val doc = configParser.parse(queryRowCountCheckString).getOrElse(Json.Null)
        val queryRowCountCheckJson = doc.hcursor.downField("queryRowCountCheck").focus.get
        val sut = parser.decode[RowCountCheck](queryRowCountCheckJson.toString)(queryRowCountDecoder)
        assert(sut ==  Right(RowCountCheck(0, "gt", QUERYROWCOUTNCHECK)))
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
              RowCountCheck(0, "gt", QUERYROWCOUTNCHECK)
            )
          )
        )
      )

      it("queryRowCount success") {
        val itemMap = Map(
          "item"     -> List("Eggs", "Milk", "Bread", "Cheese"),
          "price"    -> List(  5.49,   3.89,    4.50,     6.00),
          "quantity" -> List(    12,      5,       2,       10)
        )
        val dF= mkDF(spark, itemMap.toSeq: _*)
        val queryRowCountCheck = RowCountCheck(0, "gt", QUERYROWCOUTNCHECK)
        val queryRowCountStatMap = Map(queryRowCountCheck -> 4L)
        val queryRowCountStat = RowCountStat(queryRowCountStatMap)
        CheckTestHelper.testStat(config, queryRowCountStat, dF)
      }
    }
  }
}
*/

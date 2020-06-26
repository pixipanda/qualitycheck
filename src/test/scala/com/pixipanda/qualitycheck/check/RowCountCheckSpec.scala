package com.pixipanda.qualitycheck.check

import cats.syntax.either._
import com.pixipanda.qualitycheck.{QualityCheckConfig, TestingSparkSession}
import com.pixipanda.qualitycheck.TestHelpers._
import com.pixipanda.qualitycheck.constant.Checks.ROWCOUNTCHECK
import com.pixipanda.qualitycheck.source.Hive
import com.pixipanda.qualitycheck.stat.checkstat.RowCountStat
import io.circe.config.{parser => configParser}
import io.circe.{Json, parser}
import org.scalatest.FunSpec

class RowCountCheckSpec extends FunSpec with TestingSparkSession{

  describe("QueryRowCountCheck") {

    describe("config parsing") {

      it("basic config parsing") {
        val rowCountCheckString =
          """
            | rowCountCheck {
            |    count = 0,
            |    relation = "gt"
            | }
          """.stripMargin

        val doc = configParser.parse(rowCountCheckString).getOrElse(Json.Null)
        val rowCountCheckJson = doc.hcursor.downField("rowCountCheck").focus.get
        val sut = parser.decode[RowCountCheck](rowCountCheckJson.toString)
        assert(sut ==  Right(RowCountCheck(0, "gt", ROWCOUNTCHECK)))
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
              RowCountCheck(0, "gt", ROWCOUNTCHECK)
            )
          )
        )
      )

      it("rowCount success") {
        val itemMap = Map(
          "item"     -> List("Eggs", "Milk", "Bread", "Cheese"),
          "price"    -> List(  5.49,   3.89,    4.50,     6.00),
          "quantity" -> List(    12,      5,       2,       10)
        )
        val dF= mkDF(spark, itemMap.toSeq: _*)
        val rowCountCheck = RowCountCheck(0, "gt", ROWCOUNTCHECK)
        val rowCountStatMap = Map(rowCountCheck -> 4L)
        val rowCountStat = RowCountStat(rowCountStatMap)
        CheckTestHelper.testStat(config, rowCountStat, dF)
      }
    }
  }
}

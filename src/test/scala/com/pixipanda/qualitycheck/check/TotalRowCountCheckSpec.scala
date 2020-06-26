/*
package com.pixipanda.qualitycheck.check

import cats.syntax.either._
import com.lowes.TestingSparkSession
import com.pixipanda.qualitycheck.QualityCheckConfig
import com.pixipanda.qualitycheck.TestHelpers._
import com.pixipanda.qualitycheck.check.RowCountCheck.totalRowCountDecoder
import com.pixipanda.qualitycheck.constant.Checks.TOTALROWCOUTNCHECK
import com.pixipanda.qualitycheck.source.Hive
import com.pixipanda.qualitycheck.stat.checkstat.RowCountStat
import io.circe.config.{parser => configParser}
import io.circe.{Json, parser}
import org.scalatest.FunSpec

class TotalRowCountCheckSpec extends FunSpec with TestingSparkSession{

  describe("TotalRowCountCheck") {

    describe("config parsing") {

      it("basic config parsing") {
        val totalRowCountCheckString =
          """
            | totalRowCountCheck {
            |    count = 0,
            |    relation = "gt"
            | }
          """.stripMargin

        val doc = configParser.parse(totalRowCountCheckString).getOrElse(Json.Null)
        val totalRowCountCheckJson = doc.hcursor.downField("totalRowCountCheck").focus.get
        val sut = parser.decode[RowCountCheck](totalRowCountCheckJson.toString)(totalRowCountDecoder)
        assert(sut ==  Right(RowCountCheck(0, "gt", TOTALROWCOUTNCHECK)))
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
              RowCountCheck(0, "gt", TOTALROWCOUTNCHECK)
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
        val totalRowCountCheck = RowCountCheck(0, "gt", TOTALROWCOUTNCHECK)
        val totalRowCountStatMap = Map(totalRowCountCheck -> 4L)
        val totalRowCountStat = RowCountStat(totalRowCountStatMap)
        CheckTestHelper.testStat(config, totalRowCountStat, dF)
      }
    }
  }
}
*/

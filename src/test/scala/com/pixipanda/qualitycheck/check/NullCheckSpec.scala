package com.pixipanda.qualitycheck.check

import cats.syntax.either._
import com.pixipanda.qualitycheck.{QualityCheckConfig, TestingSparkSession}
import com.pixipanda.qualitycheck.TestHelpers._
import com.pixipanda.qualitycheck.constant.Checks.NULLCHECK
import com.pixipanda.qualitycheck.source.Hive
import com.pixipanda.qualitycheck.stat.checkstat.NullStat
import io.circe.{Json, parser}
import io.circe.config.{parser => configParser}
import org.scalatest.FunSpec

class NullCheckSpec extends FunSpec with TestingSparkSession{

  describe("NullCheck") {
    describe("config parsing") {
      it("basic config parsing") {
        val nullCheckString =
          """
            | nullCheck = ["quantity"]
          """.stripMargin
        val nullCheckJson = configParser.parse(nullCheckString).getOrElse(Json.Null)
        val sut = parser.decode[NullCheck](nullCheckJson.toString)
        assert(sut ==  Right(NullCheck(List("quantity"), NULLCHECK)))
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
              NullCheck(List("quantity"), NULLCHECK)
            )
          )
        )
      )

      it("not null success") {
        val itemMap = Map(
          "item"     -> List("Eggs", "Milk", "Bread", "Cheese"),
          "price"    -> List(  5.49,   3.89,    4.50,     6.00),
          "quantity" -> List(    12,      5,       2,       10)
        )
        val dF= mkDF(spark, itemMap.toSeq: _*)
        val nullStatMap = Map("quantity" -> 0L)
        val nullStat = NullStat(nullStatMap)
        CheckTestHelper.testStat(config, nullStat, dF)
      }

      it("null success") {
        val itemMap = Map(
          "item"     -> List("Eggs", "Milk", "Bread", "Cheese"),
          "price"    -> List(  5.49,   3.89,    4.50,     6.00),
          "quantity" -> List(    12,      5,       2,       null)
        )
        val dF= mkDF(spark, itemMap.toSeq: _*)
        val nullStatMap = Map("quantity" -> 1L)
        val nullStat = NullStat(nullStatMap)
        CheckTestHelper.testStat(config, nullStat, dF)
      }
    }
  }
}

package com.pixipanda.qualitycheck.config

import com.pixipanda.qualitycheck.constant.Checks._
import com.pixipanda.qualitycheck.{QualityCheckConfig, TestingSparkSession}
import com.pixipanda.qualitycheck.check.{DistinctCheck, DistinctRelation, NullCheck, RowCountCheck, UniqueCheck}
import com.pixipanda.qualitycheck.source.table.{Hive, Teradata}
import org.scalatest.{BeforeAndAfterAll, FunSpec}

class ConfigParserSpec extends FunSpec with BeforeAndAfterAll{

  override def beforeAll(): Unit = TestingSparkSession.configTestLog4j("OFF", "OFF")

  val hiveSource =  Hive(
    "hive",
    "db1",
    "table1",
    "query1",
    List(
      RowCountCheck(0, "gt", ROWCOUNTCHECK),
      NullCheck(List( "colA", "colB",  "colC", "colD"), NULLCHECK),
      DistinctCheck(
        List(
          DistinctRelation(List("colA"), 1600, "ge"),
          DistinctRelation(List("colZ", "colB"), 1, "ge"),
          DistinctRelation(List("colY"), 1, "ge"),
          DistinctRelation(List("colM"), 1, "ge")
        ), DISTINCTCHECK
      ),
      UniqueCheck(
        List(
          List("colA", "colB",  "colC", "colD"),
          List("colX", "colY",  "colZ"),
          List("colM", "colN")
        ), UNIQUECHECK
      )
    )
  )

  val teraDataNullQuerySource = Teradata(
    "teradata",
    "db2",
    "table2",
    null,
    List(
      RowCountCheck(0, "gt", ROWCOUNTCHECK),
      NullCheck(List( "colA", "colB",  "colC", "colD"), NULLCHECK),
      DistinctCheck(
        List(
          DistinctRelation(List("colA"), 1600, "ge"),
          DistinctRelation(List("colZ", "colB"), 1, "ge"),
          DistinctRelation(List("colY"), 1, "ge"),
          DistinctRelation(List("colM"), 1, "ge")
        ), DISTINCTCHECK
      ),
      UniqueCheck(
        List(
          List("colA", "colB",  "colC", "colD"),
          List("colX", "colY",  "colZ"),
          List("colM", "colN")
        ), UNIQUECHECK
      )
    )
  )


  describe("ConfigParser") {

    val expectedHiveConfig = QualityCheckConfig(List(hiveSource))

    it("should correctly parse simple conf string") {
      val configString =
        """
          |qualityCheck {
          |  sources = [
          |    {
          |      type = "hive"
          |      dbName = "db1"
          |      tableName = "table1"
          |      query = "query1"
          |      checks {
          |        rowCountCheck {
          |          count = 0,
          |          relation = "gt"
          |        }
          |        nullCheck = [ "colA", "colB",  "colC", "colD"]
          |        uniqueChecks = [
          |          ["colA", "colB",  "colC", "colD"],
          |          ["colX", "colY",  "colZ"]
          |          ["colM", "colN"]
          |        ]
          |        distinctChecks = [
          |          {columns = ["colA"], count = 1600, relation = "ge"},
          |          {columns = ["colZ", "colB"], count = 1, relation = "ge"},
          |          {columns = ["colY"], count = 1, relation = "ge"},
          |          {columns = ["colM"], count = 1, relation = "ge"}
          |        ]
          |      }
          |    }
          |  ]
          |}
        """.stripMargin
      val testQualityCheckConfig = ConfigParser.parseString(configString)
      assert(testQualityCheckConfig == expectedHiveConfig)
    }


    val expectedNoQueryTeraDataConfig = QualityCheckConfig(List(teraDataNullQuerySource))

    it("should correctly parse config string without query field") {

      val noQueryConfigString =
        """
          |qualityCheck  {
          |  sources = [
          |    {
          |      type = "teradata"
          |      dbName = "db2"
          |      tableName = "table2"
          |      checks {
          |        rowCountCheck {
          |          count = 0,
          |          relation = "gt"
          |        }
          |        nullCheck = ["colA", "colB",  "colC", "colD"]
          |        uniqueChecks = [
          |          ["colA", "colB",  "colC", "colD"],
          |          ["colX", "colY",  "colZ"]
          |          ["colM", "colN"]
          |        ]
          |        distinctChecks = [
          |          {columns = ["colA"], count = 1600, relation = "ge"},
          |          {columns = ["colZ", "colB"], count = 1, relation = "ge"},
          |          {columns = ["colY"], count = 1, relation = "ge"},
          |          {columns = ["colM"], count = 1, relation = "ge"}
          |        ]
          |      }
          |    }
          |  ]
          |}
        """.stripMargin

      val testQualityCheckConfig = ConfigParser.parseString(noQueryConfigString)
      assert(testQualityCheckConfig == expectedNoQueryTeraDataConfig)
    }


    val expectedConfig = QualityCheckConfig(List(hiveSource, teraDataNullQuerySource))
    it("should correctly parse simple conf file") {
      val testQualityCheckConfig = ConfigParser.parse()
      assert(testQualityCheckConfig == Right(expectedConfig))
    }
  }
}

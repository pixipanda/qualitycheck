package com.pixipanda.qualitycheck.config

import com.pixipanda.qualitycheck.constant.Checks._
import com.pixipanda.qualitycheck.{QualityCheckConfig, TestConfig, TestingSparkSession}
import com.pixipanda.qualitycheck.check.{DistinctCheck, DistinctRelation, NullCheck, RowCountCheck, UniqueCheck}
import com.pixipanda.qualitycheck.source.table.{Hive, JDBC}
import org.scalatest.{BeforeAndAfterAll, FunSpec}

class ConfigParserSpec extends FunSpec with BeforeAndAfterAll{

  override def beforeAll(): Unit = TestingSparkSession.configTestLog4j("OFF", "OFF")

  val hiveCheckOnDF = true
  val mySqlCheckOnDF = false // It is set to false as we don't have teraData setup.

  val hiveSource =  Hive(
    "hive",
    "db1",
    "table1",
    "query1",
    hiveCheckOnDF,
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

  val mySqlSourceNoQuery = JDBC(
    "mysql",
    "db2",
    "table2",
    null,
    mySqlCheckOnDF,
    TestConfig.mySqlOptions,
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

    it("should correctly parse simple conf string for hive source") {
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

      val expectedHiveConfig = QualityCheckConfig(List(hiveSource))
      val testQualityCheckConfig = ConfigParser.parseString(configString)
      assert(testQualityCheckConfig == expectedHiveConfig)
    }


    it("should correctly parse mysql config string without query field") {

      val noQueryConfigString =
        """
          |qualityCheck  {
          |  sources = [
          |    {
          |      type = "mysql"
          |      dbName = "db2"
          |      tableName = "table2"
          |      options {
          |        url = "jdbc:mysql://localhost:3306/classicmodels"
          |        user = "hduser"
          |        password = "hadoop123"
          |        driver = "com.mysql.jdbc.Driver"
          |      }
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

      val expectedNoQueryTeraDataConfig = QualityCheckConfig(List(mySqlSourceNoQuery))
      val testQualityCheckConfig = ConfigParser.parseString(noQueryConfigString)
      assert(testQualityCheckConfig == expectedNoQueryTeraDataConfig)
    }


    it("should correctly parse simple application.conf file") {
      val expectedConfig = QualityCheckConfig(List(hiveSource, mySqlSourceNoQuery))
      val testQualityCheckConfig = ConfigParser.parseQualityCheck()
      assert(testQualityCheckConfig == expectedConfig)
    }
  }
}

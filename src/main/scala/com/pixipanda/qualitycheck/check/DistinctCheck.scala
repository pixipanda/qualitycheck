package com.pixipanda.qualitycheck.check

import cats.syntax.either._
import com.pixipanda.qualitycheck.constant.Checks.DISTINCTCHECK
import com.pixipanda.qualitycheck.source.table.JDBC
import com.pixipanda.qualitycheck.stat.checkstat.{CheckStat, DistinctStat}
import com.typesafe.config.Config
import io.circe.Decoder.Result
import io.circe._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

case class DistinctRelation(columns: Seq[String], count:Int, relation: String)


case class DistinctCheck(distinctCheck: Seq[DistinctRelation], checkType: String) extends Check {


  override def getStat(df: DataFrame): CheckStat = {

    val distinctStatMap = distinctCheck.map(dCheck => {
      val distinctCount = getDistinctCount(df, dCheck.columns.toList)
      (dCheck, (distinctCount, false))
    }).toMap
    DistinctStat(distinctStatMap)
  }


  /*
   * This function computes distinct check stats for a given table.
   * Here predicate push is used. i.e data is not loaded from table to spark. Instead query is sent to the table
   */
  override def getStat(jdbcSource: JDBC):CheckStat = {

    LOGGER.info(s"Distinct check on JDBC source: ${jdbcSource.sourceType}")

    val distinctStatMap = distinctCheck.map(dRelation => {
      val query = jdbcSource.distinctQuery(dRelation.columns)
      val count = jdbcSource.predicatePushCount(query)
      dRelation -> (count, false)
    }).toMap
    DistinctStat(distinctStatMap)
  }


  /*
   *  Returns distinct count for the given set of columns on a dataFrame.
   *  Also handles duplicate column names
   */
  private def getDistinctCount(df:DataFrame, columns:List[String]):Long = {
    df.select(columns.toSet.map(col).toList: _*).distinct().count()
  }

}

object DistinctRelation  {

  val LOGGER: Logger = LoggerFactory.getLogger(getClass.getName)

  implicit val distinctCheckDecoder:Decoder[DistinctRelation] = new Decoder[DistinctRelation] {
    override def apply(c: HCursor): Result[DistinctRelation] = {
      for {
        columns <- c.downField("columns").as[Seq[String]]
        count <- c.downField("count").as[Int]
        relation <- c.downField("relation").as[String]
      }yield DistinctRelation(columns, count, relation)
    }
  }

  def parse(config: Config): DistinctRelation = {
    val columns = config.getStringList("columns").asScala.toList
    val count = config.getInt("count")
    val relation = config.getString("relation")
    DistinctRelation(columns,count, relation)
  }
}


object DistinctCheck {

  val LOGGER: Logger = LoggerFactory.getLogger(getClass.getName)

  def parse(config: Config): DistinctCheck = {
    LOGGER.info("Parsing distinctChecks")
    val distinctRelations = config.getConfigList("distinctChecks").asScala.toList.map(DistinctRelation.parse)
    DistinctCheck(distinctRelations, DISTINCTCHECK)
  }
}

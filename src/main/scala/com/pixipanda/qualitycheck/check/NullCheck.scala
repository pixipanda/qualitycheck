package com.pixipanda.qualitycheck.check

import cats.syntax.either._
import com.pixipanda.qualitycheck.constant.Checks.NULLCHECK
import com.pixipanda.qualitycheck.source.table.JDBC
import com.pixipanda.qualitycheck.stat.checkstat.{CheckStat, NullStat}
import com.typesafe.config.Config
import io.circe.Decoder.Result
import io.circe._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

case class NullCheck(columns: Seq[String], checkType: String) extends Check {

  override def getStat(df:DataFrame): CheckStat = {

    val nullStatMap = columns.map(column => {
      (column, (nullCount(df, column), false))
    }).toMap
    NullStat(nullStatMap)
  }


  /*
   * This function computes null check stats for a given table.
   * Here predicate push is used. i.e data is not loaded from table to spark. Instead query is sent to the table
   */
  override def getStat(jdbcSource: JDBC): CheckStat = {
    LOGGER.info(s"null count check on jdbc source: ${jdbcSource.sourceType}")
    val nullStatMap = columns.map(column => {
      val query = jdbcSource.nullCheckQuery(column)
      val count = jdbcSource.predicatePushCount(query)
      column -> (count, false)
    }).toMap
    NullStat(nullStatMap)
  }


  private def nullCount(df:DataFrame, column:String) = {
    df.filter(col(column).isNull or col(column) === "null").count()
  }
}

object NullCheck {

  val LOGGER: Logger = LoggerFactory.getLogger(getClass.getName)

  implicit val nullCheckDecoder:Decoder[NullCheck] = new Decoder[NullCheck] {
    override def apply(c: HCursor): Result[NullCheck] = {
      for {
        columns <- c.downField("nullCheck").as[Seq[String]]
      }yield NullCheck(columns, NULLCHECK)
    }
  }

  def parse(config: Config): NullCheck = {
    LOGGER.info("Parsing nullCheck")
    val columns = config.getStringList("nullCheck").asScala.toList
    NullCheck(columns, NULLCHECK)
  }
}

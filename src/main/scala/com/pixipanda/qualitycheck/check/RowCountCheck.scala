package com.pixipanda.qualitycheck.check

import cats.syntax.either._
import com.pixipanda.qualitycheck.constant.Checks._
import com.pixipanda.qualitycheck.source.table.Table
import com.pixipanda.qualitycheck.stat.checkstat.{CheckStat, RowCountStat}
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import io.circe.Decoder.Result
import io.circe._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

case class RowCountCheck(count:Int, relation:String, override val checkType: String) extends  Check(checkType) {

  /*
   * This function computes the row count stats for a given dataFrame
   * It returns the computed stat
   *
   */
  override def getStat(df: DataFrame): CheckStat = {
    val rowCountStatMap = Map(this -> df.count())
    RowCountStat(rowCountStatMap)
  }

  /*
   * This function computes row count stats for a given table.
   * Here predicate push is used. i.e data is not loaded from table to spark. Instead query is sent to the table
   */
  def getStat(table: Table):CheckStat = {

    val rowCountStatMap = mutable.Map[RowCountCheck, Long]()
    val query = table.rowCountQuery
    val count = predicatePushCount(table.options.get, query)(table.spark)
    rowCountStatMap.put(this, count)
    RowCountStat(rowCountStatMap.toMap)
  }

}



object  RowCountCheck {

  val LOGGER: Logger = LoggerFactory.getLogger(getClass.getName)

  implicit val rowCountDecoder:Decoder[RowCountCheck] = new Decoder[RowCountCheck] {
    override def apply(c: HCursor): Result[RowCountCheck] = {
      for {
        count <- c.downField("count").as[Int]
        relation <- c.downField("relation").as[String]
      }yield RowCountCheck(count, relation, ROWCOUNTCHECK)
    }
  }

  def parse(config: Config): RowCountCheck = {
    LOGGER.info("Parsing rowCountCheck")
    val rowCountConfig = config.getConfig("rowCountCheck")
    val count = rowCountConfig.getInt("count")
    val relation = rowCountConfig.getString("relation")
    RowCountCheck(count, relation, ROWCOUNTCHECK)
  }
}
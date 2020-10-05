package com.pixipanda.qualitycheck.check

import cats.syntax.either._
import com.pixipanda.qualitycheck.constant.Checks._
import com.pixipanda.qualitycheck.stat.checkstat.{CheckStat, RowCountStat}
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.DataFrame
import io.circe.Decoder.Result
import io.circe._

import scala.collection.mutable

case class RowCountCheck(count:Int, relation:String, override val checkType: String) extends  Check(checkType) {

  /*
   * This function computes the row count stats for a given dataFrame
   * It returns the computed stat
   *
   */
  override def getStat(df: DataFrame): CheckStat = {
    val rowCountStatMap = mutable.Map[RowCountCheck, Long]()
    val count = df.count()
    rowCountStatMap.put(this, count)
    RowCountStat(rowCountStatMap.toMap)
  }
}

object  RowCountCheck extends  LazyLogging {

  implicit val rowCountDecoder:Decoder[RowCountCheck] = new Decoder[RowCountCheck] {
    override def apply(c: HCursor): Result[RowCountCheck] = {
      for {
        count <- c.downField("count").as[Int]
        relation <- c.downField("relation").as[String]
      }yield RowCountCheck(count, relation, ROWCOUNTCHECK)
    }
  }

  def parse(config: Config): RowCountCheck = {
    val rowCountConfig = config.getConfig("rowCountCheck")
    val count = rowCountConfig.getInt("count")
    val relation = rowCountConfig.getString("relation")
    RowCountCheck(count, relation, ROWCOUNTCHECK)
  }
}
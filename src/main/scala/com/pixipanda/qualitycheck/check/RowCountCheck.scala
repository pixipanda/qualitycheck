package com.pixipanda.qualitycheck.check

import cats.syntax.either._
import com.pixipanda.qualitycheck.constant.Checks._
import com.pixipanda.qualitycheck.stat.checkstat.{CheckStat, RowCountStat}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.DataFrame
import io.circe.Decoder.Result
import io.circe._

import scala.collection.mutable

case class RowCountCheck(count:Int, relation:String, override val checkType: String) extends  Check(checkType) {

  override def getStat(df: DataFrame): Option[CheckStat] = {
    val rowCountStatMap = mutable.Map[RowCountCheck, Long]()
    val count = df.count()
    rowCountStatMap.put(this, count)
    val rowCountStat = RowCountStat(rowCountStatMap.toMap)
    Some(rowCountStat)
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
}
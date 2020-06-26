package com.pixipanda.qualitycheck.check

import cats.syntax.either._
import com.pixipanda.qualitycheck.constant.Checks.NULLCHECK
import com.pixipanda.qualitycheck.stat.checkstat.{CheckStat, NullStat}
import com.typesafe.scalalogging.LazyLogging
import io.circe.Decoder.Result
import io.circe._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.collection.mutable

case class NullCheck(columns: Seq[String], override val checkType: String) extends Check(checkType) {

  override def getStat(df:DataFrame): Option[CheckStat] = {

    val nullStatMap = mutable.Map[String, Long]()

    if(columns.nonEmpty) {
      columns.foreach(column => {
        val count = nullCount(df, column)
        nullStatMap.put(column, count)
      })
      val nullStat = NullStat(nullStatMap.toMap)
      Some(nullStat)
    } else {
      None
    }
  }

  private def nullCount(df:DataFrame, column:String) = {
    df.filter(col(column).isNull or col(column) === "null").count()
  }
}

object NullCheck extends  LazyLogging {

  implicit val nullCheckDecoder:Decoder[NullCheck] = new Decoder[NullCheck] {
    override def apply(c: HCursor): Result[NullCheck] = {
      for {
        columns <- c.downField("nullCheck").as[Seq[String]]
      }yield NullCheck(columns, NULLCHECK)
    }
  }
}

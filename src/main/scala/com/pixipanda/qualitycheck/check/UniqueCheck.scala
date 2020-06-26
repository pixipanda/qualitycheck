package com.pixipanda.qualitycheck.check

import cats.syntax.either._
import com.pixipanda.qualitycheck.constant.Checks.UNIQUECHECK
import com.pixipanda.qualitycheck.stat.checkstat.{CheckStat, UniqueStat}
import io.circe.Decoder.Result
import io.circe._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

import scala.collection.mutable

case class UniqueCheck(uniqueChecks: Seq[Seq[String]], override val checkType: String) extends  Check(checkType){

  override def getStat(df: DataFrame): Option[CheckStat] = {

    val uniqueCheckMap = mutable.Map[String, Long]()

    if (uniqueChecks.nonEmpty) {
      uniqueChecks.foreach(columns => {
        val key = columns.mkString(":")
        val duplicateCount = getDuplicateCount(df,columns.toList)
        uniqueCheckMap.put(key, duplicateCount)
      })
      val uniqueStat = UniqueStat(uniqueCheckMap.toMap)
      Some(uniqueStat)
    }else {
      None
    }
  }

  /*
    This function returns duplicate count for a given set of columns on a dataFrame
  */
  def getDuplicateCount(df:DataFrame, columns:List[String]): Long = {
    val sparkColumns = columns.map(col)
    df.select(sparkColumns: _*)
      .groupBy(sparkColumns: _*).count()
      .filter(col("count") > 1).count()
  }
}

object UniqueCheck {
  implicit  val uniqueCheckDecoder:Decoder[UniqueCheck] = new Decoder[UniqueCheck] {
    override def apply(c: HCursor): Result[UniqueCheck] = {
      for {
        uniqueChecks <- c.downField("uniqueChecks").as[Seq[Seq[String]]]
      } yield UniqueCheck(uniqueChecks, UNIQUECHECK)
    }
  }
}

package com.pixipanda.qualitycheck.check

import java.util

import cats.syntax.either._
import com.pixipanda.qualitycheck.constant.Checks.UNIQUECHECK
import com.pixipanda.qualitycheck.stat.checkstat.{CheckStat, UniqueStat}
import com.typesafe.config.Config
import io.circe.Decoder.Result
import io.circe._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

case class UniqueCheck(uniqueChecks: Seq[Seq[String]], override val checkType: String) extends  Check(checkType){

  /*
   * This function computes unique stats for a given set of columns and a dataFrame
   *
   */
  override def getStat(df: DataFrame): CheckStat = {

    val uniqueCheckMap = uniqueChecks.map(columns => {
      val key = columns.mkString(":")
      val duplicateCount = getDuplicateCount(df,columns.toList)
      (key, duplicateCount)
    }).toMap
    UniqueStat(uniqueCheckMap)
  }

  /*
   * This function returns duplicate count for a given set of columns on a dataFrame
   */
  def getDuplicateCount(df:DataFrame, columns:List[String]): Long = {
    val sparkColumns = columns.map(col)
    df.select(sparkColumns: _*)
      .groupBy(sparkColumns: _*).count()
      .filter(col("count") > 1).count()
  }
}


object UniqueCheck {

  val LOGGER: Logger = LoggerFactory.getLogger(getClass.getName)

  implicit  val uniqueCheckDecoder:Decoder[UniqueCheck] = new Decoder[UniqueCheck] {
    override def apply(c: HCursor): Result[UniqueCheck] = {
      for {
        uniqueChecks <- c.downField("uniqueChecks").as[Seq[Seq[String]]]
      } yield UniqueCheck(uniqueChecks, UNIQUECHECK)
    }
  }

  def parse(config: Config): UniqueCheck = {

    LOGGER.info("Parsing uniqueChecks")

    val uniqueChecks: Seq[Seq[String]] = config.getList("uniqueChecks")
      .unwrapped
      .asScala.toList
      .map(_.asInstanceOf[util.ArrayList[String]].asScala.toList)
    UniqueCheck(uniqueChecks, UNIQUECHECK)
  }
}

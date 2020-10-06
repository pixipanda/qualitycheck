package com.pixipanda.qualitycheck.check

import cats.syntax.either._
import com.pixipanda.qualitycheck.constant.Checks.NULLCHECK
import com.pixipanda.qualitycheck.stat.checkstat.{CheckStat, NullStat}
import com.typesafe.config.Config
import io.circe.Decoder.Result
import io.circe._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.collection.JavaConverters._

case class NullCheck(columns: Seq[String], override val checkType: String) extends Check(checkType) {

  override def getStat(df:DataFrame): CheckStat = {

    val nullStatMap = mutable.Map[String, Long]()

    columns.foreach(column => {
      val count = nullCount(df, column)
      nullStatMap.put(column, count)
    })
    NullStat(nullStatMap.toMap)
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

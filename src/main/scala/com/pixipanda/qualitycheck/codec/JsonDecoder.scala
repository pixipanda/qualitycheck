package com.pixipanda.qualitycheck.codec

import cats.syntax.either._
import com.pixipanda.qualitycheck.constant.Checks._
import com.pixipanda.qualitycheck.check._
import com.pixipanda.qualitycheck.check.RowCountCheck._
import com.pixipanda.qualitycheck.constant.DataStores._
import com.pixipanda.qualitycheck.source.Source
import com.pixipanda.qualitycheck.source.table.{Hive, Teradata}
import com.typesafe.scalalogging.LazyLogging
import io.circe.Decoder.Result
import io.circe._

import scala.collection.mutable.ListBuffer

object JsonDecoder extends LazyLogging{

  def decodeChecks(c:HCursor):List[Check] = {

    val checkBuffer = new ListBuffer[Check]

    c.downField("rowCountCheck").focus match {
      case Some(rowCountCheckJson) => parser.decode[RowCountCheck](rowCountCheckJson.toString) match {
        case Right(rowCountCheck) => checkBuffer.append(rowCountCheck)
      }
      case None => logger.info(s"rowCountCheck not specified")
    }

    c.downField("nullCheck").focus match {
      case Some(_) => parser.decode[NullCheck](c.value.toString) match {
        case Right(nullCheck) => checkBuffer.append(nullCheck)
      }
      case None => logger.info(s"nullCheck not specified")
    }

    c.downField("distinctChecks").focus match {
      case Some(distinctChecksJson) => parser.decode[List[DistinctRelation]](distinctChecksJson.toString) match {
        case Right(distinctRelation) =>
          val distinctCheck = DistinctCheck(distinctRelation, DISTINCTCHECK)
          checkBuffer.append(distinctCheck)
      }
      case None => logger.info(s"distinctChecks not specified")
    }

    c.downField("uniqueChecks").focus match {
      case Some(_) => parser.decode[UniqueCheck](c.value.toString) match {
        case Right(uniqueCheck) => checkBuffer.append(uniqueCheck)
      }
      case None => logger.info(s"uniqueChecks not specified")
    }

    checkBuffer.toList
  }


  implicit val baseTableDecoder: Decoder[Source] = new Decoder[Source] {

    private val tableDecoders = Map[String, HCursor =>Source](
      HIVE -> Hive.fromJson,
      TERADATA -> Teradata.fromJson
    )

    final def apply(hCursor: HCursor): Result[Source] = {

      for {
        tableType <- hCursor.downField("type").as[String]
      } yield {
        tableDecoders.get(tableType)
          .map(_(hCursor)) match {
          case Some(x) => x
          case None =>
            logger.error(s"Unknown Check `$tableType` in config! Choose one of: ${tableDecoders.keys.mkString(", ")}.")
            throw new RuntimeException(s"Unknown Check in config `$tableType`")
        }

      }

    }
  }
}

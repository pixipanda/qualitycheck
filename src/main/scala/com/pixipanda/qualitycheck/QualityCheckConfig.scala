package com.pixipanda.qualitycheck

import cats.syntax.either._
import com.pixipanda.qualitycheck.codec.JsonDecoder._
import com.pixipanda.qualitycheck.source.Source
import com.typesafe.config.Config
import io.circe.Decoder.Result
import io.circe._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

case class QualityCheckConfig(sources: Seq[Source])

object QualityCheckConfig {

  val LOGGER: Logger = LoggerFactory.getLogger(getClass.getName)

  implicit val qualityCheckConfigDecoder:Decoder[QualityCheckConfig] = new Decoder[QualityCheckConfig] {
    override def apply(c: HCursor): Result[QualityCheckConfig] = {
      for {
        sourcesJson <- c.downField("sources").as[Json]
        sources = parser.decode[List[Source]](sourcesJson.toString).right.get
      } yield {
        QualityCheckConfig(sources)
      }
    }
  }

  def fromJson(configJson: Json): Either[Error, QualityCheckConfig] = {
    val qualityCheckJson = configJson.hcursor.downField("qualityCheck").as[Json].right.get
    parser.decode[QualityCheckConfig](qualityCheckJson.toString)
  }

  def parse(config: Config): QualityCheckConfig = {

    LOGGER.debug(s"Pasing quality check config $config")

    val qualityCheckConfig = config.getConfig("qualityCheck")
    val sources = qualityCheckConfig.getConfigList("sources").asScala.toList.map(Source.parse)
    QualityCheckConfig(sources)
  }
}
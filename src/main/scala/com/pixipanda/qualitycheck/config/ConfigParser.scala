package com.pixipanda.qualitycheck.config

import cats.syntax.either._
import com.pixipanda.qualitycheck.QualityCheckConfig
import com.typesafe.config.{Config, ConfigFactory}
import io.circe.config.{parser => configParser}
import io.circe._

class ConfigParser {

}

object ConfigParser {

  def parseConfig(config: Config):Json = {
    configParser.parse(config).getOrElse(Json.Null)
  }

  def parseConfig(config: String):Json = {
    configParser.parse(config).getOrElse(Json.Null)
  }

  def parseJson(jsonConfig:Json): Either[Error, QualityCheckConfig] = {
    QualityCheckConfig.fromJson(jsonConfig)
  }

  def parse(): Either[Error, QualityCheckConfig] = {
    val config = ConfigFactory.load()
    val jsonConfig = parseConfig(config)
    parseJson(jsonConfig)
  }
}

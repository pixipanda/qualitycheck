package com.pixipanda.qualitycheck.config

import cats.syntax.either._
import com.pixipanda.qualitycheck.QualityCheckConfig
import com.typesafe.config.{Config, ConfigFactory}
import io.circe.config.{parser => configParser}
import io.circe._

class ConfigParser {

}

object ConfigParser {

  def parseConfig(config: Config):Either[Error, QualityCheckConfig] = {
    configParser.parse(config).flatMap(QualityCheckConfig.fromJson)
  }

  def parseConfig(config: String):Either[Error, QualityCheckConfig] = {
    configParser.parse(config).flatMap(QualityCheckConfig.fromJson)
  }

  def parseConfig(jsonConfig:Json): Either[Error, QualityCheckConfig] = {
    QualityCheckConfig.fromJson(jsonConfig)
  }

  def parse(): Either[Error, QualityCheckConfig] = {
    val config = ConfigFactory.load()
    parseConfig(config)
  }
  
}

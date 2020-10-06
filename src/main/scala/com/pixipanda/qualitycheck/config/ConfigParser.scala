package com.pixipanda.qualitycheck.config

import java.io.File

import cats.syntax.either._
import com.pixipanda.qualitycheck.QualityCheckConfig
import com.typesafe.config.{Config, ConfigFactory}
import io.circe.config.{parser => configParser}
import io.circe._
import org.slf4j.{Logger, LoggerFactory}

class ConfigParser {

}

object ConfigParser {

  val LOGGER: Logger = LoggerFactory.getLogger(getClass.getName)

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


  def parseQualityCheck(configFile: String = null): QualityCheckConfig = {
    val config = if(null != configFile) {
      ConfigFactory.parseFile(new File(configFile))
    } else {
      ConfigFactory.load()
    }
    QualityCheckConfig.parse(config)
  }


  def parseFile(file: File): QualityCheckConfig = {
    val config = ConfigFactory.parseFile(file)
    LOGGER.debug(s"Parsing config: $config")
    QualityCheckConfig.parse(config)
  }

  def parseString(configString: String): QualityCheckConfig = {
    val config = ConfigFactory.parseString(configString)
    LOGGER.debug(s"Parsing config: $config")
    QualityCheckConfig.parse(config)
  }
}

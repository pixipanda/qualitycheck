package com.pixipanda.qualitycheck.config


import com.typesafe.config.Config
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

object Options {

  val LOGGER: Logger = LoggerFactory.getLogger(getClass.getName)

  /*
    This function will parse all the options specified for any source or sink
    Return: Map collection of options
  */
  def parse(config: Config):Map[String, String] = {
    LOGGER.info("Parsing options")
    LOGGER.debug(s"Parsing options: $config")
    config.root
      .keySet
      .asScala
      .map(key => key -> config.getString(key))
      .toMap
  }
}
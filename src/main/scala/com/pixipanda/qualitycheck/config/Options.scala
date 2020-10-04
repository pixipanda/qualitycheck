package com.pixipanda.qualitycheck.config


import com.typesafe.config.Config

import scala.collection.JavaConverters._

object Options {

  /*
    This function will parse all the options specified for any source or sink
    Return: Map collection of options
  */
  def parse(config: Config):Map[String, String] = {
    config.root
      .keySet
      .asScala
      .map(key => key -> config.getString(key))
      .toMap
  }
}
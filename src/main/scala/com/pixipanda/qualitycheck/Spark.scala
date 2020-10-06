package com.pixipanda.qualitycheck

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

trait Spark {
  implicit lazy val spark: SparkSession = Spark.sparkSingleton
  implicit lazy val sc: SparkContext = spark.sparkContext
  implicit lazy val sqlContext: SQLContext = spark.sqlContext
}

object Spark {

  val LOGGER: Logger = LoggerFactory.getLogger(getClass.getName)

  private def isLinux = {
    "Linux" == System.getProperty("os.name")
  }

  lazy val sparkSingleton: SparkSession = if(isLinux) {

    val sparkConf: SparkConf = new SparkConf()
      .set("hive.exec.dynamic.partition", "true")
      .set("hive.exec.dynamic.partition.mode", "nonstrict")
      .set("spark.sql.orc.enabled", "true")
      .set("spark.ui.showConsoleProgress", "false")

    if (!sparkConf.contains("spark.master")) sparkConf.setMaster("local[*]")

    SparkSession
      .builder()
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()

  }
  else {
    SparkSession
      .builder()
      .config("spark.executor.memory", "512mb")
      .config("spark.ui.showConsoleProgress", value = false)
      .master("local[2]")
      .getOrCreate()
  }
}

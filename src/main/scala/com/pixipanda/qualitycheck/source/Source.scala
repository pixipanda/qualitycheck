package com.pixipanda.qualitycheck.source

import com.pixipanda.qualitycheck.Spark
import com.pixipanda.qualitycheck.check.Check
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.DataFrame


abstract class Source(
  sourceType: String,
  query: String
) extends  Spark with LazyLogging {

  def getChecks: Seq[Check]

  def getDF:DataFrame

  def getQueryDF:DataFrame

  def getSourceType: String

  def exists: Boolean

  def getLabel: String
}

object Source extends LazyLogging{

}

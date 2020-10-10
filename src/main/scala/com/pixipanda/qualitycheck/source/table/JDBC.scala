package com.pixipanda.qualitycheck.source.table

import com.pixipanda.qualitycheck.check.Check
import org.apache.spark.sql.DataFrame
import org.slf4j.{Logger, LoggerFactory}

final case class JDBC(sourceType: String,
                      dbName: String,
                      tableName: String,
                      query: String,
                      predicatePush: Boolean,
                      options:Map[String, String],
                      checks: Seq[Check]
                     ) extends Table(dbName, tableName, query){


  val LOGGER: Logger = LoggerFactory.getLogger(getClass.getName)

  override def getDF:DataFrame = {

    if(null != query) {
      jdbcDF(query)
    }else {
      val tableQuery = s"(SELECT * FROM $dbName.$tableName) t"
      jdbcDF(tableQuery)
    }
  }

  override def exists: Boolean = {
    val existDF = jdbcDF(existsQuery())
    existDF.select("table_name").collect().map(_.getAs[String]("table_name")).contains(tableName)
  }

  private  def existsQuery() = {
    s"information_schema.tables"
  }


  private  def jdbcDF( query: String) = {

    LOGGER.info(s"jdbc query is: $query")

    spark.read
      .format("jdbc")
      .options(
        Map(
          "url" -> options("url"),
          "user" -> options("user"),
          "password" -> options("password"),
          "dbtable" -> query,
          "driver" -> options("driver")
        )
      )
      .load()
  }


  def predicatePushCount(query: String):Long = {

    jdbcDF(query).collect()(0).getAs[Long]("count")
  }
}

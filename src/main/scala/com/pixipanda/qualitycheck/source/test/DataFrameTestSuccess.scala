package com.pixipanda.qualitycheck.source.test

import com.pixipanda.qualitycheck.check.Check

final case class DataFrameTestSuccess(
  sourceType: String,
  dbName: String,
  tableName: String,
  query: String,
  predicatePush:Boolean = false,
  options: Option[Map[String, String]],
  checks: Seq[Check]
) extends DataFrameTest(sourceType, dbName, tableName, query, checks) {

  override val data: Map[String, List[Any]] = Map(
    "item"     -> List("Eggs", "Milk", "Bread", "Cheese"),
    "price"    -> List(  5.49,   3.89,    4.50,     6.00),
    "quantity" -> List(    12,      5,       2,       10)
  )
}

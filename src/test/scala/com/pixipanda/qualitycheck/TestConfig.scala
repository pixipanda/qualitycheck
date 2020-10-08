package com.pixipanda.qualitycheck

import com.pixipanda.qualitycheck.check._
import com.pixipanda.qualitycheck.constant.Checks.{DISTINCTCHECK, NULLCHECK, ROWCOUNTCHECK, UNIQUECHECK}
import com.pixipanda.qualitycheck.source.test.{DataFrameTestFailure, DataFrameTestSuccess}

object TestConfig {
  val checkOnDF = true
  val successConfig = QualityCheckConfig(
    List(
      DataFrameTestSuccess(
        "testSpark",
        "testDb",
        "testTable",
        null,
        checkOnDF,
        None,
        List(
          RowCountCheck(0, "gt", ROWCOUNTCHECK),
          NullCheck(List("quantity"), NULLCHECK),
          DistinctCheck(List(DistinctRelation(List("item"), 2, "ge")), DISTINCTCHECK),
          UniqueCheck(List(List("item"), List("price"), List("quantity")), UNIQUECHECK)
        )
      )
    )
  )


  val failureConfig = QualityCheckConfig(
    List(
      DataFrameTestFailure(
        "testSpark",
        "testDb",
        "testTable",
        "testquery",
        checkOnDF,
        None,
        List(
          RowCountCheck(0, "gt", ROWCOUNTCHECK),
          NullCheck(List("quantity"), NULLCHECK),
          DistinctCheck(List(DistinctRelation(List("item"), 2, "ge")), DISTINCTCHECK),
          UniqueCheck(List(List("item"), List("price"), List("quantity")), UNIQUECHECK)
        )
      )
    )
  )

}

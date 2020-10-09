package com.pixipanda.qualitycheck.check

import com.pixipanda.qualitycheck.stat.checkstat.CheckStat
import com.pixipanda.qualitycheck.QualityCheckConfig
import org.apache.spark.sql.DataFrame


object CheckTestHelper extends {

  def testStat(qualityCheckConfig: QualityCheckConfig, stat: CheckStat, df: DataFrame): Unit = {
    val sources = qualityCheckConfig.sources
    sources.foreach(source => {
      val checks = source.checks
      checks.foreach(check => {
        val sut = check.getStat(df)
        assert(sut == stat)
      })
    })
  }
}

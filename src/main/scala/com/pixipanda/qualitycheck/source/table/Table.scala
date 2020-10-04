package com.pixipanda.qualitycheck.source.table

import com.pixipanda.qualitycheck.source.Source

abstract class Table(sourceType: String, query: String) extends Source(sourceType){

  def getDb: String

  def getTable: String

  def getQuery:String
}
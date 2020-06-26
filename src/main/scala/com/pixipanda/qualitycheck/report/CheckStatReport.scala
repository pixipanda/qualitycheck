package com.pixipanda.qualitycheck.report

case class CheckStatReport(statName: String,
                           columns: String,
                           relation: String,
                           configValue: String,
                           actual: String,
                           validation: String
)

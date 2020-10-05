package com.pixipanda.qualitycheck.report


final case class ColumnStatReport(statName: String,
                            columns: String,
                            relation: String,
                            configValue: String,
                            actual: String,
                            validation: String
                           )
final case class CheckStatReport(columnsStatReport: Seq[ColumnStatReport])

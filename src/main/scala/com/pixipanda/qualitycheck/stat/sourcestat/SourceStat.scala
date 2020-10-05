package com.pixipanda.qualitycheck.stat.sourcestat

import com.pixipanda.qualitycheck.stat.checkstat.CheckStat


case class SourceStat(
                       exits: Boolean = false,
                       label: String,
                       isSuccess: Boolean,
                       stats:Seq[CheckStat]
                     )

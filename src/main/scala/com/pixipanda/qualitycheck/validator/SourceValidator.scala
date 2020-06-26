package com.pixipanda.qualitycheck.validator

import com.pixipanda.qualitycheck.source.Source
import com.pixipanda.qualitycheck.validator.CheckValidator.getValidator

case class SourceValidator(label: String, validators: Seq[CheckValidator]) {

}

object SourceValidator {

  def apply(sources: Seq[Source]): Seq[SourceValidator] = {
    sources.map(source => {
      val checkValidator = source.getChecks.map(getValidator)
      SourceValidator(source.getLabel, checkValidator)
    })
  }
}
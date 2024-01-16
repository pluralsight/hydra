package hydra.kafka.util

import hydra.kafka.model.{DataClassification, ObsoleteDataClassification}
import spray.json.{JsString, JsValue}

object MetadataUtils {

  def oldToNewDataClassification(dataClassification: String): String = ObsoleteDataClassification.withNameOption(dataClassification) match {
    case Some(ObsoleteDataClassification.InternalUseOnly)        => DataClassification.InternalUse.entryName
    case Some(ObsoleteDataClassification.ConfidentialPII)        => DataClassification.Confidential.entryName
    case Some(ObsoleteDataClassification.RestrictedEmployeeData) => DataClassification.Restricted.entryName
    case Some(ObsoleteDataClassification.RestrictedFinancial)    => DataClassification.Restricted.entryName
    case _                                                       => dataClassification
  }

  private def newToOldDataClassification(dataClassification: String): Option[String] = DataClassification.withNameOption(dataClassification) match {
    case Some(DataClassification.Public)       => Some(DataClassification.Public.entryName)
    case Some(DataClassification.InternalUse)  => Some(ObsoleteDataClassification.InternalUseOnly.entryName)
    case Some(DataClassification.Confidential) => Some(ObsoleteDataClassification.ConfidentialPII.entryName)
    case _                                     => None
  }

  def oldToNewDataClassification(dataClassification: Option[JsValue]): Option[JsValue] = dataClassification map {
    case JsString(dc) => JsString(oldToNewDataClassification(dc))
    case other        => other
  }

  def deriveSubDataClassification(dataClassification: String): Option[String] =
    if (ObsoleteDataClassification.values.exists(_.entryName == dataClassification)) {
      Some(dataClassification)
    } else {
      newToOldDataClassification(dataClassification)
    }

  def deriveSubDataClassification(dataClassification: Option[JsValue]): Option[JsValue] = dataClassification match {
    case Some(JsString(dc)) => deriveSubDataClassification(dc).map(JsString(_))
    case _                  => None
  }
}

package hydra.kafka.util

import hydra.kafka.model.ObsoleteDataClassification
import spray.json.{JsString, JsValue}

object MetadataUtils {

  def oldToNewDataClassification(dataClassification: String): String = dataClassification match {
    case "InternalUseOnly"        => "InternalUse"
    case "ConfidentialPII"        => "Confidential"
    case "RestrictedFinancial"    => "Restricted"
    case "RestrictedEmployeeData" => "Restricted"
    case other                    => other
  }

  private def newToOldDataClassification(dataClassification: String): Option[String] = dataClassification match {
    case "Public"       => Some("Public")
    case "InternalUse"  => Some("InternalUseOnly")
    case "Confidential" => Some("ConfidentialPII")
    case _              => None
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

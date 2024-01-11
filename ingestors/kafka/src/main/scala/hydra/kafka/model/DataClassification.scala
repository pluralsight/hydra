package hydra.kafka.model

import enumeratum.{Enum, EnumEntry}

import scala.collection.immutable

sealed trait DataClassification extends EnumEntry
sealed trait ObsoleteDataClassification extends DataClassification

object ObsoleteDataClassification extends Enum[ObsoleteDataClassification]  {

  case object InternalUseOnly extends ObsoleteDataClassification
  case object ConfidentialPII extends ObsoleteDataClassification
  case object RestrictedFinancial extends ObsoleteDataClassification
  case object RestrictedEmployeeData extends ObsoleteDataClassification

  override val values: immutable.IndexedSeq[ObsoleteDataClassification] = findValues
}

object DataClassification extends Enum[DataClassification]  {

  case object Public extends DataClassification
  case object InternalUse extends DataClassification
  case object Confidential extends DataClassification
  case object Restricted extends DataClassification

  override val values: immutable.IndexedSeq[DataClassification] = findValues ++ ObsoleteDataClassification.values
}

sealed trait SubDataClassification extends EnumEntry

object SubDataClassification extends Enum[SubDataClassification] {

  case object Public extends SubDataClassification
  case object InternalUseOnly extends SubDataClassification
  case object ConfidentialPII extends SubDataClassification
  case object RestrictedFinancial extends SubDataClassification
  case object RestrictedEmployeeData extends SubDataClassification

  override val values: immutable.IndexedSeq[SubDataClassification] = findValues
}

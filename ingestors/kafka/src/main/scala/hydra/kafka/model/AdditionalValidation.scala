package hydra.kafka.model

import enumeratum.{Enum, EnumEntry}
import hydra.kafka.algebras.MetadataAlgebra.TopicMetadataContainer

import scala.collection.immutable

sealed trait AdditionalValidation extends EnumEntry

sealed trait MetadataAdditionalValidation extends AdditionalValidation

object MetadataAdditionalValidation extends Enum[MetadataAdditionalValidation] {
  case object replacementTopics extends MetadataAdditionalValidation

  override val values: immutable.IndexedSeq[MetadataAdditionalValidation] = findValues

  lazy val key: String = "MetadataAdditionalValidation"
}

object AdditionalValidation {
  lazy val allValidations: Option[Map[String, List[AdditionalValidation]]] =
    Some(Map(
      MetadataAdditionalValidation.key -> MetadataAdditionalValidation.values.toList
    ))

  /**
   * An OLD topic will have its metadata populated.
   * Therefore, additionalValidations=None will be picked from the metadata.
   * And no new additionalValidations will be applied on older topics.
   *
   * A NEW topic will not have a metadata object.
   * Therefore, all existing additionalValidations will be assigned.
   * Thus, additionalValidations on corresponding fields will be applied.
   *
   * Corner case: After this feature has been on STAGE/PROD for sometime and some new additionalValidations are required.
   * We need not worry about old topics as the value of additionalValidations will remain the same since the topic creation.
   * New additionalValidations should be applied only on new topics.
   * Therefore, assigning all the values under AdditionalValidation enum is reasonable.
   *
   * @param metadata a metadata object of current topic
   * @return value of additionalValidations if the topic is already existing(OLD topic) otherwise all enum values under AdditionalValidation(NEW topic)
   */
  def validations(metadata: Option[TopicMetadataContainer]): Option[Map[String, List[AdditionalValidation]]] =
    metadata.map(_.value.additionalValidations).getOrElse(AdditionalValidation.allValidations)

  def metadataValidations(metadata: Option[TopicMetadataContainer]): Option[List[MetadataAdditionalValidation]] =
    validations(metadata) flatMap { vMap =>
      vMap.get(MetadataAdditionalValidation.key)
        .map(_.asInstanceOf[List[MetadataAdditionalValidation]])
    }
}

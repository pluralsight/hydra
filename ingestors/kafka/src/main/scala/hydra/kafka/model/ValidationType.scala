package hydra.kafka.model

import enumeratum.{Enum, EnumEntry}
import hydra.kafka.algebras.MetadataAlgebra.TopicMetadataContainer

import scala.collection.immutable

sealed trait ValidationType extends EnumEntry

sealed trait MetadataValidationType extends ValidationType

object MetadataValidationType extends Enum[MetadataValidationType] {
  case object replacementTopics extends MetadataValidationType

  case object previousTopics extends MetadataValidationType

  override val values: immutable.IndexedSeq[MetadataValidationType] = findValues

  lazy val key: String = "MetadataValidationType"
}

sealed trait SchemaValidationType extends ValidationType

object SchemaValidationType extends Enum[SchemaValidationType] {
  case object defaultInRequiredField extends SchemaValidationType

  case object timestampMillis extends SchemaValidationType

  override val values: immutable.IndexedSeq[SchemaValidationType] = findValues

  lazy val key: String = "SchemaValidationType"
}

object ValidationType {
  lazy val allValidations: Option[Map[String, List[ValidationType]]] =
    Some(Map(
      MetadataValidationType.key -> MetadataValidationType.values.toList,
      SchemaValidationType.key   -> SchemaValidationType.values.toList,
    ))

  /**
   * An OLD topic will have its metadata populated.
   * Therefore, validations=None will be picked from the metadata.
   * And no new validations will be applied on older topics.
   *
   * A NEW topic will not have a metadata object.
   * Therefore, all existing validations will be assigned.
   * Thus, validations on corresponding fields will be applied.
   *
   * Corner case: After this feature has been on STAGE/PROD for sometime and some new validations are required.
   * We need not worry about old topics as the value of validations will remain the same since the topic creation.
   * New validations should be applied only on new topics.
   * Therefore, assigning all the values under ValidationType enum is reasonable.
   *
   * @param metadata a metadata object of current topic
   * @return value of validations if the topic is already existing(OLD topic) otherwise all enum values under ValidationType(NEW topic)
   */
  def validations(metadata: Option[TopicMetadataContainer]): Option[Map[String, List[ValidationType]]] =
    metadata.map(_.value.validations).getOrElse(ValidationType.allValidations)

  def metadataValidations(metadata: Option[TopicMetadataContainer]): Option[List[MetadataValidationType]] =
    validations(metadata) flatMap { vMap =>
      vMap.get(MetadataValidationType.key)
        .map(_.asInstanceOf[List[MetadataValidationType]])
    }

  def isPresent(metadata: Option[TopicMetadataContainer], validationType: ValidationType): Boolean =
    validations(metadata).exists(m => m.keys.exists(k => m.get(k).exists(_.contains(validationType))))
}

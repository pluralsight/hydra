package hydra.kafka.model

import java.time.Instant

import cats.data.NonEmptyList
import hydra.core.marshallers.StreamType
import org.apache.avro.Schema

sealed trait DataClassification
case object Public extends DataClassification
case object InternalUseOnly extends DataClassification
case object ConfidentialPII extends DataClassification
case object RestrictedFinancial extends DataClassification
case object RestrictedEmployeeData extends DataClassification

sealed trait ContactMethod

final case class Email(address: String) extends ContactMethod
final case class Slack(channel: String) extends ContactMethod

final case class Schemas(key: Schema, value: Schema)

final case class Subject private (value: String) extends AnyVal

object Subject {
  private val kafkaValidCharacterRegex = """^[a-zA-Z0-9_\-.]+$""".r

  def createValidated(value: String): Option[Subject] =
    if (kafkaValidCharacterRegex.pattern.matcher(value).matches())
      Some(new Subject(value))
    else None
}

case class TopicMetadataV2Request(
    subject: Subject,
    schemas: Schemas,
    streamType: StreamType,
    deprecated: Boolean,
    dataClassification: DataClassification,
    contact: NonEmptyList[ContactMethod],
    createdDate: Instant,
    parentSubjects: List[Subject],
    notes: Option[String]
)

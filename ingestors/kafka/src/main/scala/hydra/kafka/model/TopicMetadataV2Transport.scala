package hydra.kafka.model

import java.time.Instant

import cats.data.NonEmptyList
import eu.timepit.refined._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.string._
import hydra.common.logging.LoggingAdapter
import hydra.core.marshallers.{History, StreamType}
import hydra.kafka.model.TopicMetadataV2Transport.Subject
import org.apache.avro.{Schema, SchemaBuilder}

sealed trait DataClassification
case object Public extends DataClassification
case object InternalUseOnly extends DataClassification
case object ConfidentialPII extends DataClassification
case object RestrictedFinancial extends DataClassification
case object RestrictedEmployeeData extends DataClassification

sealed trait ContactMethod

object ContactMethod {

  def create(s: String): Option[ContactMethod] =
    Email.create(s).orElse(Slack.create(s))

  type EmailRegex =
    MatchesRegex[
      W.`"""^[A-Z0-9a-z._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,64}$"""`.T
    ]
  private type EmailAddress = String Refined EmailRegex

  final case class Email(address: EmailAddress) extends ContactMethod

  object Email {

    def create(value: String): Option[Email] =
      refineV[EmailRegex](value).toOption.map(Email.apply)
  }

  type SlackRegex = MatchesRegex[W.`"""^[#][^\\sA-Z]{1,79}$"""`.T]

  private type SlackChannel =
    String Refined SlackRegex

  final case class Slack(channel: SlackChannel) extends ContactMethod

  object Slack {

    def create(value: String): Option[Slack] =
      refineV[SlackRegex](value).toOption.map(Slack.apply)
  }
}

object Schemas {
  def emptySchema: Schema = SchemaBuilder
    .record("SchemaNotFound")
    .fields()
    .endRecord()
}

final case class Schemas(key: Schema, value: Schema)

final case class TopicMetadataV2Transport(
    subject: Subject,
    schemas: Schemas,
    streamType: StreamType,
    deprecated: Boolean,
    dataClassification: DataClassification,
    contact: NonEmptyList[ContactMethod],
    createdDate: Instant,
    parentSubjects: List[Subject],
    notes: Option[String]
) {

  def toKeyAndValue: (TopicMetadataV2Key, TopicMetadataV2Value) = {
    val key = TopicMetadataV2Key(subject)
    val value = TopicMetadataV2Value(
      streamType,
      deprecated,
      dataClassification,
      contact,
      createdDate,
      parentSubjects,
      notes
    )
    (key, value)
  }
}

object TopicMetadataV2Transport extends LoggingAdapter {
  type SubjectRegex = MatchesRegex[W.`"""^[a-zA-Z0-9_\\-\\.]+$"""`.T]
  type Subject = String Refined SubjectRegex

  object Subject {

    def createValidated(value: String): Option[Subject] = {
      refineV[SubjectRegex](value).toOption
    }

    val invalidFormat = "Invalid Subject. Subject may contain only alphanumeric characters, hyphens(-), underscores(_), and periods(.)"
  }

  def fromKeyAndValue(k: TopicMetadataV2Key, v: TopicMetadataV2Value, keySchema: Option[Schema], valueSchema: Option[Schema]): TopicMetadataV2Transport = {
    TopicMetadataV2Transport(
      k.subject,
      Schemas(keySchema.getOrElse(Schemas.emptySchema), valueSchema.getOrElse(Schemas.emptySchema)),
      v.streamType,
      v.deprecated,
      v.dataClassification,
      v.contact,
      v.createdDate,
      v.parentSubjects,
      v.notes
    )
  }
}

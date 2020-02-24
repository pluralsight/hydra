package hydra.kafka.model

import java.time.Instant

import cats.data.NonEmptyList
import eu.timepit.refined._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.string._
import hydra.core.marshallers.StreamType
import hydra.kafka.model.TopicMetadataV2Request.Subject
import org.apache.avro.Schema

sealed trait DataClassification
case object Public extends DataClassification
case object InternalUseOnly extends DataClassification
case object ConfidentialPII extends DataClassification
case object RestrictedFinancial extends DataClassification
case object RestrictedEmployeeData extends DataClassification

sealed trait ContactMethod

object ContactMethod {

  type EmailRegex =
    MatchesRegex[W.`"""^[A-Z0-9a-z._%+-]+@[A-Za-z0-9.-]+.[A-Za-z]{2,64}$"""`.T]
  private type EmailAddress = String Refined EmailRegex

  final case class Email(address: EmailAddress) extends ContactMethod

  object Email {

    def create(value: String): Option[Email] =
      refineV[EmailRegex](value).toOption.map(Email.apply)
  }

  type SlackRegex = MatchesRegex[W.`"""^[#][^sA-Z]{1,79}$"""`.T]
  private type SlackChannel =
    String Refined SlackRegex

  final case class Slack(channel: SlackChannel) extends ContactMethod

  object Slack {

    def create(value: String): Option[Slack] =
      refineV[SlackRegex](value).toOption.map(Slack.apply)
  }
}

final case class Schemas(key: Schema, value: Schema)

final case class TopicMetadataV2Request(
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

object TopicMetadataV2Request {
  type SubjectRegex = MatchesRegex[W.`"""^[a-zA-Z0-9_-.]+$"""`.T]
  type Subject = String Refined SubjectRegex

  object Subject {

    def createValidated(value: String): Option[Subject] = {
      refineV[SubjectRegex](value).toOption
    }
  }
}

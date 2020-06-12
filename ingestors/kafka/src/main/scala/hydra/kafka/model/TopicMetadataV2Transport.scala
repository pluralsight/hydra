package hydra.kafka.model

import java.time.Instant

import cats.data.NonEmptyList
import eu.timepit.refined._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.string._
import hydra.core.marshallers.StreamType
import hydra.kafka.algebras.MetadataAlgebra.TopicMetadataContainer
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

final case class Schemas(key: Schema, value: Schema)

final case class TopicMetadataV2Request(
    schemas: Schemas,
    streamType: StreamType,
    deprecated: Boolean,
    dataClassification: DataClassification,
    contact: NonEmptyList[ContactMethod],
    createdDate: Instant,
    parentSubjects: List[Subject],
    notes: Option[String]
) {

  def toValue: TopicMetadataV2Value = {
    TopicMetadataV2Value(
      streamType,
      deprecated,
      dataClassification,
      contact,
      createdDate,
      parentSubjects,
      notes
    )
  }
}

object TopicMetadataV2Request {
  type SubjectRegex = MatchesRegex[W.`"""^[a-zA-Z0-9_\\-\\.]+$"""`.T]
  type Subject = String Refined SubjectRegex

  object Subject {

    def createValidated(value: String): Option[Subject] = {
      refineV[SubjectRegex](value).toOption
    }

    val invalidFormat = "Invalid Subject. Subject may contain only alphanumeric characters, hyphens(-), underscores(_), and periods(.)"
  }
}

final case class MaybeSchemas(key: Option[Schema], value: Option[Schema])
final case class TopicMetadataV2Response(
                                          subject: Subject,
                                          schemas: MaybeSchemas,
                                          streamType: StreamType,
                                          deprecated: Boolean,
                                          dataClassification: DataClassification,
                                          contact: NonEmptyList[ContactMethod],
                                          createdDate: Instant,
                                          parentSubjects: List[Subject],
                                          notes: Option[String]
                                        )
object TopicMetadataV2Response {
  def fromTopicMetadataContainer(m: TopicMetadataContainer): TopicMetadataV2Response = {
    val (k, v, keySchema, valueSchema) = (m.key, m.value, m.keySchema, m.valueSchema)
    TopicMetadataV2Response(
      k.subject,
      MaybeSchemas(keySchema, valueSchema),
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



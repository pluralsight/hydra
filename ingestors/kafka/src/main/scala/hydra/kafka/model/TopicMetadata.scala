package hydra.kafka.model

import java.time.Instant
import java.util.UUID

import cats.data.Validated.{Invalid, Valid}
import cats.data.{NonEmptyList, Validated}
import cats.syntax.all._
import cats.{Applicative, ApplicativeError, Monad, MonadError}
import fs2.kafka.Headers
import hydra.avro.convert.{ISODateConverter, IsoDate}
import hydra.core.marshallers._
import hydra.kafka.model.TopicMetadataV2Request.Subject
import org.apache.avro.generic.GenericRecord
import spray.json.DefaultJsonProtocol
import vulcan.generic._
import vulcan.refined._
import vulcan.{AvroError, AvroNamespace, Codec}

import scala.util.control.NoStackTrace

/**
  * Created by alexsilva on 3/30/17.
  */

case class TopicMetadata(
    subject: String,
    schemaId: Int,
    streamType: String,
    derived: Boolean,
    deprecated: Option[Boolean],
    dataClassification: String,
    contact: String,
    additionalDocumentation: Option[String],
    notes: Option[String],
    id: UUID,
    createdDate: org.joda.time.DateTime
)

object TopicMetadataV2 {

  def getSchemas[F[_]: ApplicativeError[*[_], Throwable]]: F[Schemas] = {
    (
      Validated.fromEither(TopicMetadataV2Key.codec.schema).toValidatedNel,
      Validated.fromEither(TopicMetadataV2Value.codec.schema).toValidatedNel
    ).mapN(Schemas.apply) match {
      case Valid(s) =>
        Applicative[F].pure(s)
      case Invalid(e) =>
        ApplicativeError[F, Throwable].raiseError(MetadataAvroSchemaFailure(e))
    }
  }

  def encode[F[_]: MonadError[*[_], Throwable]](
      key: TopicMetadataV2Key,
      value: Option[TopicMetadataV2Value],
      headers: Option[Headers] = None
  ): F[(GenericRecord, Option[GenericRecord], Option[Headers])] = {
    Monad[F]
      .pure {
        val valueResult: Option[Either[AvroError, Any]] = value.map(TopicMetadataV2Value.codec.encode)
        (
          Validated
            .fromEither(TopicMetadataV2Key.codec.encode(key))
            .toValidatedNel,
          valueResult match {
            case Some(e: Either[AvroError, Any]) =>
              Validated
                .fromEither(e)
                .toValidatedNel
            case None => None.validNel
          }
          ).tupled.toEither.leftMap { a =>
          MetadataAvroSchemaFailure(a)
        }
      }
      .rethrow
      .flatMap {
        case (k: GenericRecord, v: GenericRecord) => Monad[F].pure((k, Option(v), headers))
        case (k, v) =>
          MonadError[F, Throwable].raiseError(
            AvroEncodingFailure(
              NonEmptyList.of(k, v, headers).map(_.getClass.getSimpleName)
            )
          )
      }
  }

  def decode[F[_]: MonadError[*[_], Throwable]](
                                               key: GenericRecord,
                                               value: Option[GenericRecord]
                                               ): F[(TopicMetadataV2Key, Option[TopicMetadataV2Value])] = {
    getSchemas[F].flatMap { schemas =>
      Monad[F]
        .pure {
          val valueResult: Option[Either[AvroError, TopicMetadataV2Value]] = value.map(TopicMetadataV2Value.codec.decode(_, schemas.value))
          (
            Validated
              .fromEither(TopicMetadataV2Key.codec.decode(key, schemas.key))
              .toValidatedNel,
            valueResult match {
              case Some(Left(avroError)) =>
               avroError.invalidNel
              case Some (Right(topicMetadataV2Value)) =>
                Some(topicMetadataV2Value).validNel
              case None => None.validNel
            }
            ).tupled.toEither
            .leftMap { a =>
              MetadataAvroSchemaFailure(a)
            }
        }
        .rethrow
    }
  }

  final case class MetadataAvroSchemaFailure(errors: NonEmptyList[AvroError])
      extends NoStackTrace

  final case class AvroEncodingFailure(unexpectedTypes: NonEmptyList[String])
      extends NoStackTrace

}

@AvroNamespace("_hydra.v2")
final case class TopicMetadataV2Key(
    subject: Subject
)

object TopicMetadataV2Key {

  implicit val codec: Codec[TopicMetadataV2Key] =
    Codec.derive[TopicMetadataV2Key]

}

@AvroNamespace("_hydra.v2")
final case class TopicMetadataV2Value(
    streamType: StreamTypeV2,
    deprecated: Boolean,
    deprecatedDate: Option[Instant],
    dataClassification: DataClassification,
    contact: NonEmptyList[ContactMethod],
    createdDate: Instant,
    parentSubjects: List[Subject],
    notes: Option[String],
    teamName: Option[String]
)

object TopicMetadataV2Value {

  implicit val streamTypeCodec: Codec[StreamTypeV2] =
    Codec.deriveEnum[StreamTypeV2](
      symbols = List("Event", "Entity", "Telemetry"),
      encode = {
        case StreamTypeV2.Event      => "Event"
        case StreamTypeV2.Entity     => "Entity"
        case StreamTypeV2.Telemetry  => "Telemetry"
      },
      decode = {
        case "Event"     => Right(StreamTypeV2.Event)
        case "Entity"    => Right(StreamTypeV2.Entity)
        case "Telemetry" => Right(StreamTypeV2.Telemetry)
        case other       => Left(AvroError(s"$other is not a StreamTypeV2"))
      }
    )

  implicit val dataClassificationCodec: Codec[DataClassification] =
    Codec.deriveEnum[DataClassification](
      symbols = List(
        "Public",
        "InternalUseOnly",
        "ConfidentialPII",
        "RestrictedFinancial",
        "RestrictedEmployeeData"
      ),
      encode = {
        case Public                 => "Public"
        case InternalUseOnly        => "InternalUseOnly"
        case ConfidentialPII        => "ConfidentialPII"
        case RestrictedFinancial    => "RestrictedFinancial"
        case RestrictedEmployeeData => "RestrictedEmployeeData"
      },
      decode = {
        case "Public"                 => Right(Public)
        case "InternalUseOnly"        => Right(InternalUseOnly)
        case "ConfidentialPII"        => Right(ConfidentialPII)
        case "RestrictedFinancial"    => Right(RestrictedFinancial)
        case "RestrictedEmployeeData" => Right(RestrictedEmployeeData)
        case other                    => Left(AvroError(s"$other is not a DataClassification"))
      }
    )

  private implicit val instantCodec: Codec[Instant] = Codec.string.imap { str =>
    new ISODateConverter()
      .fromCharSequence(
        str,
        org.apache.avro.SchemaBuilder.builder.stringType,
        IsoDate
      )
      .toInstant
  } { instant => instant.toString }

  private implicit val contactMethodCodec: Codec[ContactMethod] =
    Codec.derive[ContactMethod]

  implicit val codec: Codec[TopicMetadataV2Value] =
  Codec.record[TopicMetadataV2Value](
    name = "TopicMetadataV2Value",
    namespace = "_hydra.v2"
  ) {
    field =>
      (field("streamType", _.streamType),
        field("deprecated", _.deprecated),
        field("deprecatedDate", _.deprecatedDate, default = Some(None)),
        field("dataClassification", _.dataClassification),
        field("contact", _.contact),
        field("createdDate", _.createdDate),
        field("parentSubjects", _.parentSubjects),
        field("notes", _.notes),
        field("teamName", _.teamName, default = Some(None))
        ).mapN(TopicMetadataV2Value.apply)
  }
}

package hydra.kafka.model

import java.time.Instant
import java.util.UUID

import cats.data.Validated.{Invalid, Valid}
import cats.data.{NonEmptyList, Validated}
import cats.implicits._
import cats.{Applicative, ApplicativeError, Monad, MonadError}
import hydra.avro.convert.{ISODateConverter, IsoDate}
import hydra.core.marshallers._
import hydra.kafka.model.TopicMetadataV2Request.Subject
import org.apache.avro.generic.GenericRecord
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
    ).mapN(Schemas) match {
      case Valid(s) => Applicative[F].pure(s)
      case Invalid(e) =>
        ApplicativeError[F, Throwable].raiseError(MetadataAvroSchemaFailure(e))
    }
  }

  def encode[F[_]: MonadError[*[_], Throwable]](
      key: TopicMetadataV2Key,
      value: TopicMetadataV2Value
  ): F[(GenericRecord, GenericRecord)] = {
    Monad[F]
      .pure(
        (
          Validated
            .fromEither(TopicMetadataV2Key.codec.encode(key))
            .toValidatedNel,
          Validated
            .fromEither(TopicMetadataV2Value.codec.encode(value))
            .toValidatedNel
        ).tupled.toEither.leftMap{ a =>
          MetadataAvroSchemaFailure(a)
        }
      )
      .rethrow
      .flatMap {
        case (k: GenericRecord, v: GenericRecord) => Monad[F].pure((k, v))
        case (k, v) =>
          MonadError[F, Throwable].raiseError(
            AvroEncodingFailure(
              NonEmptyList.of(k, v).map(_.getClass.getSimpleName)
            )
          )
      }
  }

  def decode[F[_]: MonadError[*[_], Throwable]](
                                               key: GenericRecord,
                                               value: GenericRecord
                                               ): F[(TopicMetadataV2Key, TopicMetadataV2Value)] = {
    getSchemas[F].flatMap { schemas =>
      Monad[F]
        .pure(
          (
            Validated
              .fromEither(TopicMetadataV2Key.codec.decode(key, schemas.key))
              .toValidatedNel,
            Validated
              .fromEither(TopicMetadataV2Value.codec.decode(value, schemas.value))
              .toValidatedNel
            ).tupled.toEither
            .leftMap{ a =>
            MetadataAvroSchemaFailure(a)
          }
        )
        .rethrow
        .flatMap {
          case (k, v) =>
            Monad[F].pure((k, v))
        }
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
    streamType: StreamType,
    deprecated: Boolean,
    dataClassification: DataClassification,
    contact: NonEmptyList[ContactMethod],
    createdDate: Instant,
    parentSubjects: List[Subject],
    notes: Option[String]
)

object TopicMetadataV2Value {

  implicit val streamTypeCodec: Codec[StreamType] =
    Codec.deriveEnum[StreamType](
      symbols = List("Notification", "CurrentState", "History", "Telemetry"),
      encode = {
        case History      => "History"
        case Telemetry    => "Telemetry"
        case CurrentState => "CurrentState"
        case Notification => "Notification"
      },
      decode = {
        case "History"      => Right(History)
        case "Telemetry"    => Right(Telemetry)
        case "CurrentState" => Right(CurrentState)
        case "Notification" => Right(Notification)
        case other          => Left(AvroError(s"$other is not a StreamType"))
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
    Codec.derive[TopicMetadataV2Value]
}

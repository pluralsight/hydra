package hydra.kafka.model

import java.time.Instant

import cats.data.{NonEmptyList, Validated}
import cats.data.Validated.{Invalid, Valid}
import cats.{Applicative, ApplicativeError, Monad, MonadError}
import cats.implicits._
import org.apache.avro.generic.GenericRecord
import vulcan.{AvroError, Codec}
import vulcan.generic._

import scala.util.control.NoStackTrace

object TopicConsumer {
  object TopicConsumerKey {
    implicit val codec: Codec[TopicConsumerKey] = Codec.derive[TopicConsumerKey]
  }
  final case class TopicConsumerKey(topicName: String, consumerGroupName: String)

  object TopicConsumerValue {
    implicit val codec: Codec[TopicConsumerValue] = Codec.derive[TopicConsumerValue]
  }
  final case class TopicConsumerValue(lastCommit: Instant)

  def getSchemas[F[_]: ApplicativeError[*[_], Throwable]]: F[Schemas] = {
    (
      Validated.fromEither(TopicConsumerKey.codec.schema).toValidatedNel,
      Validated.fromEither(TopicConsumerValue.codec.schema).toValidatedNel
      ).mapN(Schemas.apply) match {
      case Valid(s) =>
        Applicative[F].pure(s)
      case Invalid(e) =>
        ApplicativeError[F, Throwable].raiseError(TopicConsumerAvroSchemaFailure(e))
    }
  }

  def encode[F[_]: MonadError[*[_], Throwable]](
                                                 key: TopicConsumerKey,
                                                 value: Option[TopicConsumerValue]
                                               ): F[(GenericRecord, Option[GenericRecord])] = {
    Monad[F]
      .pure {
        val valueResult: Option[Either[AvroError, Any]] = value.map(TopicConsumerValue.codec.encode)
        (
          Validated
            .fromEither(TopicConsumerKey.codec.encode(key))
            .toValidatedNel,
          valueResult match {
            case Some(e: Either[AvroError, Any]) =>
              Validated
                .fromEither(e)
                .toValidatedNel
            case None => None.validNel
          }
          ).tupled.toEither.leftMap { a =>
          TopicConsumerAvroSchemaFailure(a)
        }
      }
      .rethrow
      .flatMap {
        case (k: GenericRecord, v: GenericRecord) => Monad[F].pure((k, Option(v)))
        case (k: GenericRecord, _) => Monad[F].pure((k, None))
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
                                                 value: Option[GenericRecord]
                                               ): F[(TopicConsumerKey, Option[TopicConsumerValue])] = {
    getSchemas[F].flatMap { schemas =>
      Monad[F]
        .pure {
          val valueResult: Option[Either[AvroError, TopicConsumerValue]] = value.map(TopicConsumerValue.codec.decode(_, schemas.value))
          (
            Validated
              .fromEither(TopicConsumerKey.codec.decode(key, schemas.key))
              .toValidatedNel,
            valueResult match {
              case Some(Left(avroError)) =>
                avroError.invalidNel
              case Some (Right(topicConsumerValue)) =>
                Some(topicConsumerValue).validNel
              case None => None.validNel
            }
            ).tupled.toEither
            .leftMap { a =>
              TopicConsumerAvroSchemaFailure(a)
            }
        }
        .rethrow
    }
  }

  final case class TopicConsumerAvroSchemaFailure(errors: NonEmptyList[AvroError])
    extends NoStackTrace

  final case class AvroEncodingFailure(unexpectedTypes: NonEmptyList[String])
    extends NoStackTrace

}

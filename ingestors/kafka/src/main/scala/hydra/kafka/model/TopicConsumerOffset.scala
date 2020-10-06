package hydra.kafka.model

import cats.data.Validated.{Invalid, Valid}
import cats.data.{NonEmptyList, Validated}
import cats.implicits._
import cats.{Applicative, ApplicativeError, Monad, MonadError}
import org.apache.avro.generic.GenericRecord
import vulcan.generic._
import vulcan.{AvroError, Codec}

import scala.util.control.NoStackTrace

object TopicConsumerOffset {
  object TopicConsumerOffsetKey {
    implicit val codec: Codec[TopicConsumerOffsetKey] = Codec.derive[TopicConsumerOffsetKey]
  }
  final case class TopicConsumerOffsetKey(topicName: String, partition: Int)

  object TopicConsumerOffsetValue {
    implicit val codec: Codec[TopicConsumerOffsetValue] = Codec.derive[TopicConsumerOffsetValue]
  }
  final case class TopicConsumerOffsetValue(offset: Long)

  def getSchemas[F[_]: ApplicativeError[*[_], Throwable]]: F[Schemas] = {
    (
      Validated.fromEither(TopicConsumerOffsetKey.codec.schema).toValidatedNel,
      Validated.fromEither(TopicConsumerOffsetValue.codec.schema).toValidatedNel
      ).mapN(Schemas.apply) match {
      case Valid(s) =>
        Applicative[F].pure(s)
      case Invalid(e) =>
        ApplicativeError[F, Throwable].raiseError(TopicConsumerOffsetAvroSchemaFailure(e))
    }
  }

  def encode[F[_]: MonadError[*[_], Throwable]](
                                                 key: TopicConsumerOffsetKey,
                                                 value: TopicConsumerOffsetValue
                                               ): F[(GenericRecord, GenericRecord)] = {
    Monad[F]
      .pure {
        (
          Validated
            .fromEither(TopicConsumerOffsetKey.codec.encode(key))
            .toValidatedNel,
          Validated
            .fromEither(TopicConsumerOffsetValue.codec.encode(value))
            .toValidatedNel,
          ).tupled.toEither.leftMap { a =>
          TopicConsumerOffsetAvroSchemaFailure(a)
        }
      }
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
                                                 value: Option[GenericRecord]
                                               ): F[(TopicConsumerOffsetKey, Option[TopicConsumerOffsetValue])] = {
    getSchemas[F].flatMap { schemas =>
      Monad[F]
        .pure {
          val valueResult: Option[Either[AvroError, TopicConsumerOffsetValue]] = value.map(TopicConsumerOffsetValue.codec.decode(_, schemas.value))
          (
            Validated
              .fromEither(TopicConsumerOffsetKey.codec.decode(key, schemas.key))
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
              TopicConsumerOffsetAvroSchemaFailure(a)
            }
        }
        .rethrow
    }
  }

  final case class TopicConsumerOffsetAvroSchemaFailure(errors: NonEmptyList[AvroError])
    extends NoStackTrace

  final case class AvroEncodingFailure(unexpectedTypes: NonEmptyList[String])
    extends NoStackTrace

}

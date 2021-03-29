package hydra.kafka.algebras

import hydra.avro.convert.SimpleStringToGenericRecord
import hydra.kafka.algebras.KafkaClientAlgebra.PublishResponse
import SimpleStringToGenericRecord._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import hydra.kafka.model.Schemas
import vulcan.Codec
import vulcan.generic._
import cats.data.Validated
import cats.data.Validated.{Invalid, Valid}
import cats.effect.concurrent.Ref
import cats.effect.{Concurrent, Sync}
import cats.implicits._
import io.chrisdavenport.log4cats.Logger
import org.apache.avro.generic.GenericRecord
import spray.json._

final case class HydraTag(name: String, description: String)
private final case class HydraTagName(name: String)
private final case class HydraTagDescription(description: String)

private object HydraTagName extends SprayJsonSupport with DefaultJsonProtocol {
  implicit val hydraTagNameFormat: RootJsonFormat[HydraTagName] = jsonFormat1(HydraTagName.apply)
  implicit val hydraTagNameCodec: Codec[HydraTagName] = Codec.derive[HydraTagName]
}

private object HydraTagDescription extends SprayJsonSupport with DefaultJsonProtocol {
  implicit val hydraTagDescriptionFormat: RootJsonFormat[HydraTagDescription] = jsonFormat1(HydraTagDescription.apply)
  implicit val hydraTagDescriptionCodec: Codec[HydraTagDescription] = Codec.derive[HydraTagDescription]
}

object  HydraTag extends SprayJsonSupport with DefaultJsonProtocol  {
  implicit val hydraTagFormat: RootJsonFormat[HydraTag] = jsonFormat2(HydraTag.apply)
  implicit val hydraTagCodec: Codec[HydraTag] = Codec.derive[HydraTag]

    def getSchemas: Schemas = {
        (
          Validated.fromEither(HydraTagName.hydraTagNameCodec.schema).toValidatedNel,
          Validated.fromEither(HydraTagDescription.hydraTagDescriptionCodec.schema).toValidatedNel
        ).mapN(Schemas.apply) match {
        case Valid(s) =>
          s
        case Invalid(e) =>
          throw new Exception(s"HydraTag getSchemas Error(s): ${e.map(a => a.message).toList}")
      }
    }

}


trait TagsAlgebra[F[_]] {
  def createOrUpdateTag(tagsRequest: HydraTag,
                              kafkaClientAlgebra: KafkaClientAlgebra[F]): F[Either[KafkaClientAlgebra.PublishError,PublishResponse]]
  def getAllTags: F[List[HydraTag]]
}
object TagsAlgebra {

  def make[F[_]: Sync: Concurrent: Logger](tagsTopic: String,
                                           tagsClient: String,
                                           kafkaClientAlgebra: KafkaClientAlgebra[F]): F[TagsAlgebra[F]] = {
    val tagsStream: fs2.Stream[F,(GenericRecord,Option[GenericRecord])] =
                    kafkaClientAlgebra.consumeMessages(tagsTopic, tagsClient, commitOffsets = false).map(record => (record._1, record._2))
    for {
      ref <- Ref[F].of(TagsStorageFacade.empty)
      _ <- Concurrent[F].start(tagsStream.flatMap{
        case (key, value) =>
        fs2.Stream.eval {
          value match {
            case Some(value) => {
              ref.update(_.addMetadata(HydraTag(key.get("name").toString, value.get("description").toString)))
            }
            case None => {
              ref.update(_.removeMetadata(key.toString))
            }
          }
        }
        case e => {
          fs2.Stream.eval(Logger[F].error(s"Unexpected return from Kafka: ${e.toString()}"))
        }
      }.recoverWith {
        case e =>
        fs2.Stream.eval(Logger[F].warn(s"Error in TagsAlgebra"))
      }.compile.drain)
      algebra <- getTagsAlgebra(ref, tagsTopic)
    } yield algebra
  }

  private def getTagsAlgebra[F[_]: Sync: Logger](cache: Ref[F, TagsStorageFacade], tagsTopic: String): F[TagsAlgebra[F]] = {
    Sync[F].delay {
      new TagsAlgebra[F] {
        override def getAllTags: F[List[HydraTag]] = cache.get.map(_.tagsMap.map(tm => HydraTag(tm._1, tm._2)).toList)

        override def createOrUpdateTag(tagsRequest: HydraTag,
                                               kafkaClientAlgebra: KafkaClientAlgebra[F]): F[Either[KafkaClientAlgebra.PublishError,PublishResponse]] = {
          val tagsSchemas = HydraTag.getSchemas
          val genericRecordKey = tagsRequest.toJson.toString.toGenericRecordSimple(tagsSchemas.key)
            .getOrElse(throw new Exception(s"createOrUpdateTag GenericRecordSimple error: Key ${tagsSchemas.key}"))
          val genericRecordValue = tagsRequest.toJson.toString.toGenericRecordSimple(tagsSchemas.value)
            .getOrElse(throw new Exception(s"createOrUpdateTag GenericRecordSimple error: Value ${tagsSchemas.value}"))
          kafkaClientAlgebra.publishMessage((genericRecordKey, Some(genericRecordValue), None), tagsTopic)
        }
      }
    }
  }

}

private case class TagsStorageFacade(tagsMap: Map[String, String]) {
  def addMetadata(hydraTag: HydraTag): TagsStorageFacade = {
    this.copy(this.tagsMap + (hydraTag.name -> hydraTag.description))
  }
  def removeMetadata(key: String): TagsStorageFacade = {
    this.copy(this.tagsMap - key)
  }
  def getAllTags: List[HydraTag] = tagsMap.map(tm => HydraTag(tm._1,tm._2)).toList
}

private object TagsStorageFacade {
  def empty: TagsStorageFacade = TagsStorageFacade(Map.empty)
}

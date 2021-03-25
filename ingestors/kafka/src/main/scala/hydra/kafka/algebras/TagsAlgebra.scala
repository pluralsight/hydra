package hydra.kafka.algebras

import hydra.avro.convert.SimpleStringToGenericRecord
import hydra.kafka.algebras.KafkaClientAlgebra.PublishResponse
import SimpleStringToGenericRecord._
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
import spray.json.{JsArray, JsBoolean, JsNull, JsNumber, JsObject, JsString, JsValue, RootJsonFormat}

final case class HydraTag(name: String, description: String)

object  HydraTag {
  implicit val codec: Codec[HydraTag] = Codec.derive[HydraTag]
    def getSchemas: Schemas = {
        (
          Validated.fromEither(HydraTag.codec.schema).toValidatedNel,
          Validated.fromEither(HydraTag.codec.schema).toValidatedNel
        ).mapN(Schemas.apply) match {
        case Valid(s) =>
          s
        case Invalid(e) =>
          throw new Exception(s"HydraTag getSchemas Error(s): ${e.map(a => a.message).toList}")
      }
    }

  implicit object HydraTagFormat extends RootJsonFormat[HydraTag] {
    override def write(obj: HydraTag): JsValue = {
      val hydraMap: Map[String,JsValue] = Map(obj.name -> JsString(obj.description))
      JsObject(fields = hydraMap)
    }

    override def read(json: JsValue): HydraTag = {
      json match {
        case JsObject(e) if(e.size == 1) =>
          val key = e.keySet.head
          val value = e.get(key).getOrElse(throw new Exception(s"Unable to get value for key: $key")).toString()
          HydraTag(key, value)
      }
    }
  }
}


trait TagsAlgebra[F[_]] {
  def createOrUpdateTag(tagsTopic: String, tagsRequest: HydraTag,
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
              ref.update(_.addMetadata(HydraTag(key.toString, value.toString)))
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
      algebra <- getTagsAlgebra(ref)
    } yield algebra
  }

  private def getTagsAlgebra[F[_]: Sync: Logger](cache: Ref[F, TagsStorageFacade]): F[TagsAlgebra[F]] = {
    Sync[F].delay {
      new TagsAlgebra[F] {
        override def getAllTags: F[List[HydraTag]] = cache.get.map(_.tagsMap.map(tm => HydraTag(tm._1, tm._2)).toList)

        override def createOrUpdateTag(tagsTopic: String, tagsRequest: HydraTag,
                                               kafkaClientAlgebra: KafkaClientAlgebra[F]): F[Either[KafkaClientAlgebra.PublishError,PublishResponse]] = {
          val tagsSchemas = HydraTag.getSchemas
          kafkaClientAlgebra.publishMessage(
            (tagsRequest.name.toGenericRecordSimple(tagsSchemas.key, useStrictValidation = true)
              .getOrElse(throw new Exception(s"createOrUpdateTag GenericRecordSimple error: Key ${tagsSchemas.key}"))
              , Some(
              tagsRequest.description
                .toGenericRecordSimple(tagsSchemas.value, useStrictValidation = true)
                .getOrElse(throw new Exception(s"createOrUpdateTag GenericRecordSimple error: Value ${tagsSchemas.value}")))
              , None),tagsTopic)
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

//object TopicConsumer {

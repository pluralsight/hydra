package hydra.kafka.marshallers

import akka.http.scaladsl.marshalling.{Marshaller, Marshalling}
import akka.http.scaladsl.model.ContentTypes
import akka.util.ByteString
import hydra.core.marshallers.HydraJsonSupport
import hydra.kafka.model.TopicMetadata
import org.apache.kafka.common.{Node, PartitionInfo}
import spray.json.{JsNumber, JsObject, JsString, JsValue, JsonFormat}

import scala.concurrent.Future

/**
  * Created by alexsilva on 3/19/17.
  */
trait HydraKafkaJsonSupport extends HydraJsonSupport {

  implicit val topicMetadataFormat = jsonFormat7(TopicMetadata)

  implicit object NodeJsonFormat extends JsonFormat[Node] {
    override def write(node: Node): JsValue = {
      JsObject(
        "id" -> JsString(node.idString),
        "host" -> JsString(node.host),
        "port" -> JsNumber(node.port)
      )
    }

    override def read(json: JsValue): Node = ???
  }

  implicit object PartitionInfoJsonFormat extends JsonFormat[PartitionInfo] {

    import spray.json._

    override def write(p: PartitionInfo): JsValue = {
      JsObject(
        "partition" -> JsNumber(p.partition()),
        "leader" -> p.leader().toJson,
        "isr" -> JsArray(p.inSyncReplicas().toJson)
      )
    }

    override def read(json: JsValue): PartitionInfo = ???
  }


  implicit val stringFormat = Marshaller[String, ByteString] { ec ⇒
    s =>
      Future.successful {
        List(Marshalling.WithFixedContentType(ContentTypes.`application/json`, () => ByteString(s)))
      }
  }
}
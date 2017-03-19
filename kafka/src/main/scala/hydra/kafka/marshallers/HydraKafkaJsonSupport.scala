package hydra.kafka.marshallers

import hydra.core.marshallers.HydraJsonSupport
import org.apache.kafka.common.{Node, PartitionInfo}
import spray.json.{JsNumber, JsObject, JsString, JsValue, JsonFormat}

/**
  * Created by alexsilva on 3/19/17.
  */
trait HydraKafkaJsonSupport extends HydraJsonSupport {

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

}
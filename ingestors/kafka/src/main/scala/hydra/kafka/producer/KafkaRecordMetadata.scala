package hydra.kafka.producer

import hydra.core.transport.{AckStrategy, RecordMetadata}
import spray.json.{
  DefaultJsonProtocol,
  JsNumber,
  JsObject,
  JsString,
  JsValue,
  RootJsonFormat
}

/**
  * Created by alexsilva on 2/22/17.
  */
case class KafkaRecordMetadata(
    offset: Long,
    timestamp: Long,
    destination: String,
    partition: Int,
    deliveryId: Long,
    ackStrategy: AckStrategy
) extends RecordMetadata

object KafkaRecordMetadata extends DefaultJsonProtocol {

  def apply(
      kmd: org.apache.kafka.clients.producer.RecordMetadata,
      deliveryId: Long,
      ackStrategy: AckStrategy
  ): KafkaRecordMetadata = {
    KafkaRecordMetadata(
      kmd.offset(),
      kmd.timestamp(),
      kmd.topic(),
      kmd.partition(),
      deliveryId,
      ackStrategy
    )
  }

  implicit object AckStrategyFormat extends RootJsonFormat[AckStrategy] {
    override def write(obj: AckStrategy): JsValue = JsString(obj.toString)

    override def read(json: JsValue): AckStrategy =
      AckStrategy.apply(json.convertTo[String]).getOrElse(AckStrategy.NoAck)
  }

  implicit val mdFormat = jsonFormat6(KafkaRecordMetadata.apply)
}

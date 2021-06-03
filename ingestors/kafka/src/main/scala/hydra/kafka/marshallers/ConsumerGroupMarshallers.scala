package hydra.kafka.marshallers

import java.time.Instant
import spray.json._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import hydra.kafka.algebras.{ConsumerGroupsAlgebra, KafkaAdminAlgebra}
import hydra.kafka.algebras.ConsumerGroupsAlgebra.{ConsumerTopics, PartitionOffset, TopicConsumers}
import hydra.kafka.algebras.KafkaAdminAlgebra.{LagOffsets, Offset, TopicAndPartition}

trait ConsumerGroupMarshallers extends DefaultJsonProtocol with SprayJsonSupport {

  implicit object InstantFormat extends RootJsonFormat[Instant] {
    override def write(obj: Instant): JsValue = JsString(obj.toString)

    override def read(json: JsValue): Instant = Instant.now()
  }

  implicit val topicAndPartition: RootJsonFormat[TopicAndPartition] = jsonFormat2(TopicAndPartition.apply)
  implicit val offset: JsonFormat[Offset] = jsonFormat1(Offset.apply)
  implicit val lag: RootJsonFormat[LagOffsets] = jsonFormat2(LagOffsets.apply)
  implicit val partitionOffset: RootJsonFormat[PartitionOffset] = jsonFormat4(PartitionOffset.apply)
  implicit val topicFormat: RootJsonFormat[ConsumerGroupsAlgebra.Topic] = jsonFormat3(ConsumerGroupsAlgebra.Topic)
  implicit val consumerFormat: RootJsonFormat[ConsumerGroupsAlgebra.Consumer] = jsonFormat2(ConsumerGroupsAlgebra.Consumer)

  implicit val consumerTopicsFormat: RootJsonFormat[ConsumerTopics] = jsonFormat2(ConsumerTopics)
  implicit val topicConsumersFormat: RootJsonFormat[TopicConsumers] = jsonFormat2(TopicConsumers)

}

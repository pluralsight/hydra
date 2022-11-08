package hydra.kafka.marshallers

import java.time.Instant
import spray.json.{RootJsonFormat, _}
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import hydra.core.transport.AckStrategy
import hydra.kafka.algebras.{ConsumerGroupsAlgebra, KafkaAdminAlgebra}
import hydra.kafka.algebras.ConsumerGroupsAlgebra.{ConsumerTopics, DetailedConsumerGroup, DetailedTopicConsumers, PartitionOffset, TopicConsumers}
import hydra.kafka.algebras.KafkaAdminAlgebra.{LagOffsets, Offset, TopicAndPartition}
import hydra.kafka.serializers.TopicMetadataV2Parser.IntentionallyUnimplemented

trait ConsumerGroupMarshallers extends DefaultJsonProtocol with SprayJsonSupport {

  implicit object InstantFormat extends RootJsonFormat[Instant] {
    override def write(obj: Instant): JsValue = JsString(obj.toString)

    override def read(json: JsValue): Instant = Instant.now()
  }

  implicit val topicAndPartition: RootJsonFormat[TopicAndPartition] = jsonFormat2(TopicAndPartition.apply)
  implicit val offset: JsonFormat[Offset] = jsonFormat1(Offset.apply)
  implicit val lag: RootJsonFormat[LagOffsets] = jsonFormat2(LagOffsets.apply)
  implicit val partitionOffset: RootJsonFormat[PartitionOffset] = jsonFormat4(PartitionOffset.apply)

  implicit object detailedConsumerGroupFormat extends RootJsonFormat[DetailedConsumerGroup] {
    override def write(detailedConsumerGroup: DetailedConsumerGroup): JsValue = JsObject(List(
      Some("topicName" -> JsString(detailedConsumerGroup.topicName)),
      Some("consumerGroupName" -> JsString(detailedConsumerGroup.consumerGroupName)),
      Some("lastCommit" -> InstantFormat.write(detailedConsumerGroup.lastCommit)),
      if (detailedConsumerGroup.offsetInformation.isEmpty) None else Some("offsetInformation" ->
        JsArray(detailedConsumerGroup.offsetInformation.sortBy(_.partition).map(partitionOffset.write).toVector)),
      if (detailedConsumerGroup.totalLag.isEmpty) None else Some("totalLag" -> JsNumber(detailedConsumerGroup.totalLag.getOrElse(0L))),
      if (detailedConsumerGroup.state.isEmpty) None else Some("State" -> JsString(detailedConsumerGroup.state.getOrElse("Unknown")))
    ).flatten.toMap)

    override def read(json: JsValue): DetailedConsumerGroup = jsonFormat6(DetailedConsumerGroup.apply).read(json)
  }

  implicit object detailedTopicConsumersFormat extends RootJsonFormat[DetailedTopicConsumers] {
    override def write(obj: DetailedTopicConsumers): JsValue = JsObject(
      List(
        Some("consumers" -> JsArray(obj.consumers.map(consumer => detailedConsumerGroupFormat.write(consumer)).toVector))
      ).flatten.toMap
    )

    override def read(json: JsValue): DetailedTopicConsumers = jsonFormat2(DetailedTopicConsumers.apply).read(json)
  }

  implicit object consumerFormat extends RootJsonFormat[ConsumerGroupsAlgebra.Consumer] {
    override def write(obj: ConsumerGroupsAlgebra.Consumer): JsValue = JsObject(List(
      Some("consumerGroupName" -> JsString(obj.consumerGroupName)),
      Some("lastCommit" -> InstantFormat.write(obj.lastCommit)),
      if(obj.state.isEmpty) None else Some("state" -> JsString(obj.state.getOrElse("Unknown")))
    ).flatten.toMap)

    override def read(json: JsValue): ConsumerGroupsAlgebra.Consumer = throw IntentionallyUnimplemented
  }

  implicit val consumerTopicsFormat: RootJsonFormat[ConsumerTopics] = jsonFormat2(ConsumerTopics)
  implicit val topicConsumersFormat: RootJsonFormat[TopicConsumers] = jsonFormat2(TopicConsumers)

}

package hydra.kafka.marshallers

import java.time.Instant

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import hydra.kafka.algebras.ConsumerGroupsAlgebra
import hydra.kafka.algebras.ConsumerGroupsAlgebra.{ConsumerTopics, TopicConsumers}
import spray.json.{DefaultJsonProtocol, JsString, JsValue, RootJsonFormat}

trait ConsumerGroupMarshallers extends DefaultJsonProtocol with SprayJsonSupport {

  implicit object InstantFormat extends RootJsonFormat[Instant] {
    override def write(obj: Instant): JsValue = JsString(obj.toString)

    override def read(json: JsValue): Instant = Instant.now()
  }

  implicit val topicFormat: RootJsonFormat[ConsumerGroupsAlgebra.Topic] = jsonFormat2(ConsumerGroupsAlgebra.Topic)
  implicit val consumerFormat: RootJsonFormat[ConsumerGroupsAlgebra.Consumer] = jsonFormat2(ConsumerGroupsAlgebra.Consumer)

  implicit val consumerTopicsFormat: RootJsonFormat[ConsumerTopics] = jsonFormat2(ConsumerTopics)
  implicit val topicConsumersFormat: RootJsonFormat[TopicConsumers] = jsonFormat2(TopicConsumers)

}

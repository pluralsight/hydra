package hydra.kafka.model

import hydra.core.marshallers.{CurrentState, History, HydraJsonSupport, Notification, StreamType, Telemetry}
import hydra.kafka.algebras.MetadataAlgebra.TopicMetadataV2Container
import hydra.kafka.serializers.Errors.StreamTypeInvalid
import hydra.kafka.serializers.TopicMetadataV2Parser
import io.pileworx.akka.http.rest.hal.{Link, ResourceBuilder}
import spray.json._

trait TopicMetadataV2Adapter extends HydraJsonSupport {

  import TopicMetadataV2Parser._

  implicit val newVale = TopicMetadataV2Parser.StreamTypeFormat

  private implicit val topicMetadataV2KeyFormat: RootJsonFormat[TopicMetadataV2Key] = jsonFormat1(TopicMetadataV2Key.apply)
  private implicit val topicMetadataV2ValueFormat: RootJsonFormat[TopicMetadataV2Value] = jsonFormat7(TopicMetadataV2Value.apply)
  private implicit val topicMetadataV2ContainerFormat: RootJsonFormat[TopicMetadataV2Container] = jsonFormat2(TopicMetadataV2Container)

  def streamLink(rel: String, id: String): (String, Link) = rel -> Link(href = s"/v2/streams/$id")

  def schemaLink(subject: String): (String, Link) =
    "hydra-schema" -> Link(href = s"/v2/schemas/$subject")

  def toResource(tm: TopicMetadataV2Container): JsValue =
    ResourceBuilder(
      withData = Some(tm.toJson),
      withLinks =
        Some(Map(streamLink("self", tm.key.subject.value), schemaLink(tm.key.subject.value)))
    ).build
}

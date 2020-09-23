package hydra.kafka.model

import hydra.core.marshallers.HydraJsonSupport
import io.pileworx.akka.http.rest.hal.{Link, ResourceBuilder}
import spray.json._

/**
  * Created by alexsilva on 3/30/17.
  */
trait TopicMetadataAdapter extends HydraJsonSupport {

  implicit val topicMetadataFormat = jsonFormat11(TopicMetadata)

  def streamLink(rel: String, id: String) = rel -> Link(href = s"/streams/$id")

  def schemaLink(subject: String) =
    "hydra-schema" -> Link(href = s"/schemas/$subject")

  def toResource(tm: TopicMetadata): JsValue =
    ResourceBuilder(
      withData = Some(tm.toJson),
      withLinks =
        Some(Map(streamLink("self", tm.subject), schemaLink(tm.subject)))
    ).build
}

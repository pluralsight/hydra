package hydra.kafka.model

import hydra.core.marshallers.StreamType
import spray.json.JsObject

case class TopicMetadataV2Request(
                                  subject: String,
                                  keySchema: JsObject,
                                  valueSchema: JsObject,
                                  streamType: StreamType,
                                  derived: Boolean,
                                  deprecated: Option[Boolean],
                                  dataClassification: String,
                                  contact: String,
                                  additionalDocumentation: Option[String],
                                  notes: Option[String])

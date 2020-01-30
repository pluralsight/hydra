package hydra.kafka.model

import java.time.Instant

import hydra.core.marshallers.StreamType
import org.apache.avro.Schema

sealed trait ContactMethod

final case class Email(address: String) extends ContactMethod
final case class Slack(channel: String) extends ContactMethod

final case class Schemas(key: Schema, value: Schema)

case class TopicMetadataV2Request(
                                  subject: String,
                                  schemas: Schemas,
                                  streamType: StreamType,
                                  deprecated: Boolean,
                                  dataClassification: String,
                                  contact: List[ContactMethod],
                                  createdDate: Instant,
                                  parentSubjects: List[String],
                                  notes: Option[String])
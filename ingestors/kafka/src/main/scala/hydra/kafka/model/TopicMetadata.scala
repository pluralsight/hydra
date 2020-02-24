package hydra.kafka.model

import java.time.Instant
import java.util.UUID

import cats.data.NonEmptyList
import com.sksamuel.avro4s.{AvroName, AvroNamespace}
import hydra.core.marshallers.StreamType

/**
  * Created by alexsilva on 3/30/17.
  */
case class TopicMetadata(
    subject: String,
    schemaId: Int,
    streamType: String,
    derived: Boolean,
    deprecated: Option[Boolean],
    dataClassification: String,
    contact: String,
    additionalDocumentation: Option[String],
    notes: Option[String],
    id: UUID,
    createdDate: org.joda.time.DateTime
)

@AvroNamespace("_hydra.v2")
@AvroName("metadata")
final case class TopicMetadataV2Key(
    subject: Subject
)

@AvroNamespace("_hydra.v2")
@AvroName("metadata")
final case class TopicMetadataV2Value(
    streamType: StreamType,
    deprecated: Boolean,
    dataClassification: DataClassification,
    contact: NonEmptyList[ContactMethod],
    createdDate: Instant,
    parentSubjects: List[Subject],
    notes: Option[String]
)

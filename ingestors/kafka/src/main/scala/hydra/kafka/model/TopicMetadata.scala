package hydra.kafka.model

import java.time.Instant
import java.util.UUID

import cats.data.NonEmptyList
import com.sksamuel.avro4s.{AvroName, AvroNamespace, AvroSchema, RecordFormat}
import hydra.core.marshallers.StreamType
import org.apache.avro.Schema

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

object TopicMetadataV2Key {
  val schema: Schema = AvroSchema[TopicMetadataV2Key]

  val recordFormat: RecordFormat[TopicMetadataV2Key] =
    RecordFormat[TopicMetadataV2Key]
}

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

object TopicMetadataV2Value {
  val schema: Schema = AvroSchema[TopicMetadataV2Value]

  val recordFormat: RecordFormat[TopicMetadataV2Value] =
    RecordFormat[TopicMetadataV2Value]
}

package hydra.kafka.model

import java.util.UUID

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

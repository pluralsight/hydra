package hydra.core.ingest

/**
  * Created by alexsilva on 12/3/16.
  */
object IngestionParams {


  val HYDRA_INGESTOR_PARAM = "hydra-ingestor"

  /**
    * Use when targetting a specific ingestor by name.  If this parameter is present in the request, the message will
    * not be broadcast via [[hydra.core.notification.Publish]].  That phase of the protocol will be skipped and the
    * ingestion will begin with a [[hydra.core.protocol.Validate]] message.
    *
    * The value of this parameter needs to match to the name of a registered ingestor.
    */
  val HYDRA_INGESTOR_TARGET_PARAM = "hydra-ingestor-target"


  @deprecated(" Use IngestionParams.HYDRA_SCHEMA_PARAM instead", "0.7")
  val HYDRA_AVRO_SCHEMA_PARAM = "hydra-avro-schema"

  /**
    * Which schema to use.  The correct syntax is resource_type:location
    *
    * For instance classpath:schema.avsc.
    *
    * Schemas can also be looked up from the registry, by using the 'registry' resource type and providing a subject
    * name with an optional version number appended to it, using the format subject#version.  Example: registry:test#1
    *
    * If no version is present, the latest schema version will be used.  Keep in mind that this requires an explicit
    * call to the schema registry to get the latest version for a subject.  Since this call cannot be cached,
    * there are significant performance hits to using this parameter without a version number.
    */
  val HYDRA_SCHEMA_PARAM = "hydra-schema"

  /**
    * A parameter value that matches a requet to KafkaIngestor
    */
  val KAFKA = "kafka"

  val HYDRA_KAFKA_TOPIC_PARAM = "hydra-kafka-topic"
  val HYDRA_REQUEST_LABEL_PARAM = "hydra-request-label"
  val HYDRA_RECORD_KEY_PARAM = "hydra-record-key"
  val HYDRA_RECORD_FORMAT_PARAM = "hydra-record-format"
  val HYDRA_RETRY_STRATEGY = "hydra-retry-strategy"
  val HYDRA_VALIDATION_STRATEGY = "hydra-validation"
  val STRICT = "strict"
  val RELAXED = "relaxed"
}

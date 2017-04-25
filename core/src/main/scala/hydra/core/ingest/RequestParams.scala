package hydra.core.ingest

/**
  * Created by alexsilva on 12/3/16.
  */
object RequestParams {

  /**
    * Use when targetting a specific ingestor by name.  If this parameter is present in the request, the message will
    * not be broadcast via [[hydra.core.notification.Publish]].  That phase of the protocol will be skipped and the
    * ingestion will begin with a [[hydra.core.protocol.Validate]] message.
    *
    * The value of this parameter needs to match to the name of a registered ingestor.
    */
  val HYDRA_INGESTOR_PARAM = "hydra-ingestor"

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
  val HYDRA_REQUEST_ID_PARAM = "hydra-request-id"
  val HYDRA_RECORD_KEY_PARAM = "hydra-record-key"
  val HYDRA_RECORD_FORMAT_PARAM = "hydra-record-format"
  val HYDRA_RETRY_STRATEGY = "hydra-retry-strategy"
  val HYDRA_VALIDATION_STRATEGY = "hydra-validation"
  val STRICT = "strict"
  val RELAXED = "relaxed"

  /**
    * If present and set to true, a single request with a JSON array payload
    * will be split into multiple HydraRequests each corresponding to an element of the array.
    */
  val SPLIT_JSON_ARRAY = "split-json-array"

  val REPLY_TO = "reply-to"

  /**
    * Determines when the ingestion is considered completed.
    *
    * Actual behavior is up each ingestor, but as a general rule:
    *
    * If "none", ingestors should not wait for a [[hydra.core.protocol.RecordProduced]] message
    * from the underlying producer and instead must reply with an [[hydra.core.protocol.IngestorCompleted]] message
    * as soon as the request is sent to the producer.
    *
    * If "explicit", ingestors should only send an [[hydra.core.protocol.IngestorCompleted]] message
    * after receiving a [[hydra.core.protocol.RecordProduced]] message from the producer.
    *
    * Do not block waiting for a RecordProduced message.
    *
    * Be aware that setting this parameter to true raises the possibility of clients receiving ingestion timeouts.
    *
    */
  val HYDRA_ACK_STRATEGY = "hydra-ack"


  /**
    * Can be 'detailed' or 'simple' (default).
    *
    * If 'detailed', every request produces a more detailed response including duration for each ingestor, etc.
    *
    * A 'simple' response produces only status codes.
    */
  val HYDRA_RESPONSE_FORMAT = "hydra-response-format"

  val HYDRA_INGEST_TIMEOUT = "hydra-ingest-timeout"

}

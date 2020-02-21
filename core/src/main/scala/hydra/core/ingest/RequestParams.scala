package hydra.core.ingest

/**
  * Created by alexsilva on 12/3/16.
  */
// $COVERAGE-OFF$Disabling highlighting by default until a workaround for https://issues.scala-lang.org/browse/SI-8596 is found
object RequestParams {

  /**
    * Used when targeting a specific ingestor by name.
    *
    * If this parameter is present in the request, the message will
    * not be broadcast via a [[hydra.core.protocol.Publish]] message.
    *
    * Instead, the Publish phase of the protocol will be skipped and the
    * ingestion will begin with a [[hydra.core.protocol.Join]] message sent directly to that ingestor.
    *
    * Caution: The value of this parameter needs to match the name of an ingestor
    * available in the ingestor registry.
    */
  val HYDRA_INGESTOR_PARAM = "hydra-ingestor"

  /**
    * Which schema to use to validate and encode the request.
    *
    * The expected syntax is resource_type:location.  For instance, http://schema.avsc.
    *
    * Schemas can also be looked up from Confluent's schema registry, by using the 'registry' resource type and providing a subject
    * name with an optional version number appended to it, using the format subject#version.  Example: registry:test#1
    *
    * If no version is present, the latest schema version will be used.  Keep in mind that this requires an explicit
    * call to the schema registry to get the latest version for a subject.  Since this call cannot be cached,
    * there are significant performance hits to using this parameter without a version number.
    */
  val HYDRA_SCHEMA_PARAM = "hydra-schema"

  /**
    * The underlying topic this message should be sent to.
    * Required when using the Kafka ingestor.
    */
  val HYDRA_KAFKA_TOPIC_PARAM = "hydra-kafka-topic"

  /**
    * The JSON Path for a field in the body that specifies the message key.
    *
    */
  val HYDRA_RECORD_KEY_PARAM = "hydra-record-key"

  /**
    * The format of the message serialization.  The possible formats are avro, string and json.
    *
    * For Kafka, 'avro' is the record format.
    */
  val HYDRA_RECORD_FORMAT_PARAM = "hydra-record-format"

  /**
    * Defines how Avro messages should be validated.
    *
    * If set to `strict` (default), any incoming payload with fields that are not present in the schema will be rejected as invalid.
    * If set to `relaxed`, standard Avro validation rules will apply.
    */
  val HYDRA_VALIDATION_STRATEGY = "hydra-validation"

  /**
    * Param value for strict validation.
    */
  val STRICT = "strict"

  /**
    * Param value for relaxed validation.
    */
  val RELAXED = "relaxed"

  /**
    * Determines when clients receive an ingestion completed response.
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
    * Ingestor implementations should never block waiting for a RecordProduced message.
    *
    * Be aware that setting this parameter to "explicit" raises the possibility of clients receiving ingestion timeouts.
    *
    */
  val HYDRA_ACK_STRATEGY = "hydra-ack"

  val HydraClientId = "hydra-client-id"

  /**
    * The amount of time, in milliseconds, to wait before a timeout error is returned to the client.
    *
    */
  val HYDRA_INGEST_TIMEOUT = "hydra-ingest-timeout"

}
// $COVERAGE-ON$

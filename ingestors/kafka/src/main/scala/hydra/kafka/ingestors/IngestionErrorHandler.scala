package hydra.kafka.ingestors

import akka.actor.Actor
import com.pluralsight.hydra.avro.JsonToAvroConversionException
import hydra.common.config.ConfigSupport._
import hydra.avro.registry.JsonToAvroConversionExceptionWithMetadata
import hydra.common.config.ConfigSupport
import hydra.core.ingest.RequestParams.HYDRA_KAFKA_TOPIC_PARAM
import hydra.core.protocol.GenericIngestionError
import hydra.core.transport.Transport.Deliver
import hydra.kafka.producer.AvroRecord
import org.apache.avro.Schema
import spray.json.DefaultJsonProtocol

import scala.io.Source

/**
  * A base error handler that sends a summary of ingestion errors  to a deadleter topic in Kafka.
  *
  * Created by alexsilva on 12/22/16.
  */
class IngestionErrorHandler
    extends Actor
    with ConfigSupport
    with DefaultJsonProtocol {

  import spray.json._

  private implicit val ec = context.dispatcher

  private implicit val hydraIngestionErrorInfoFormat = jsonFormat6(
    HydraIngestionErrorInfo
  )

  private val errorTopic = applicationConfig
    .getStringOpt("ingest.error-topic")
    .getOrElse("_hydra_ingest_errors")

  private lazy val kafkaTransport = context
    .actorSelection(
      applicationConfig
        .getStringOpt(s"transports.kafka.path")
        .getOrElse(s"/user/service/kafka_transport")
    )

  private val errorSchema = new Schema.Parser()
    .parse(Source.fromResource("schemas/HydraIngestError.avsc").mkString)

  override def receive: Receive = {
    case error: GenericIngestionError =>
      kafkaTransport ! Deliver(buildPayload(error))
  }

  private[ingestors] def buildPayload(
      err: GenericIngestionError
  ): AvroRecord = {
    val schema: Option[String] = err.cause match {
      case e: JsonToAvroConversionException             => Some(e.getSchema.toString)
      case e: JsonToAvroConversionExceptionWithMetadata => Some(e.location)
      case e: Exception                                 => None
    }

    val topic = err.request.metadataValue(HYDRA_KAFKA_TOPIC_PARAM)

    val errorInfo = HydraIngestionErrorInfo(
      err.ingestor,
      topic,
      err.cause.getMessage,
      err.request.metadata,
      schema,
      err.request.payload
    ).toJson.compactPrint

    AvroRecord(
      errorTopic,
      errorSchema,
      topic,
      errorInfo,
      err.request.ackStrategy
    )
  }
}

case class HydraIngestionErrorInfo(
    ingestor: String,
    destination: Option[String],
    errorMessage: String,
    metadata: Map[String, String],
    schema: Option[String],
    payload: String
)

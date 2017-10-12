package hydra.kafka.ingestors

import akka.actor.Actor
import com.pluralsight.hydra.avro.JsonToAvroConversionException
import configs.syntax._
import hydra.avro.registry.ConfluentSchemaRegistry
import hydra.common.config.ConfigSupport
import hydra.core.avro.JsonToAvroConversionExceptionWithMetadata
import hydra.core.avro.schema.SchemaResourceLoader
import hydra.core.ingest.RequestParams.HYDRA_KAFKA_TOPIC_PARAM
import hydra.core.protocol.{HydraIngestionError, ProduceOnly}
import hydra.kafka.producer.AvroRecord
import spray.json.DefaultJsonProtocol

/**
  * A base error handler that sends a summary of ingestion errors  to a deadleter topic in Kafka.
  *
  * Created by alexsilva on 12/22/16.
  */
class IngestionErrorHandler extends Actor with ConfigSupport with DefaultJsonProtocol {

  import spray.json._

  private implicit val hydraIngestionErrorInfoFormat = jsonFormat6(HydraIngestionErrorInfo)

  private val errorTopic = applicationConfig.get[String]("ingest.error-topic").valueOrElse("__hydra_ingest_errors")

  private lazy val kafkaTransport = context.actorSelection(applicationConfig.get[String](s"transports.kafka.path")
    .valueOrElse(s"/user/service/kafka_transport"))

  private val errorSchema = {
    val name = applicationConfig.get[String]("ingest.error.schema")
      .valueOrElse("classpath:schemas/HydraIngestError.avsc")
    val registry = ConfluentSchemaRegistry.forConfig(applicationConfig)
    val loader = new SchemaResourceLoader(registry.registryUrl, registry.registryClient)
    loader.getResource(name).schema
  }


  override def receive: Receive = {
    case error: HydraIngestionError => kafkaTransport ! ProduceOnly(buildPayload(error))
  }

  private[ingestors] def buildPayload(err: HydraIngestionError): AvroRecord = {
    val schema: Option[String] = err.error match {
      case e: JsonToAvroConversionException => Some(e.getSchema.toString)
      case e: JsonToAvroConversionExceptionWithMetadata => Some(e.res.location)
      case e: Exception => None
    }

    val topic = err.request.metadataValue(HYDRA_KAFKA_TOPIC_PARAM)

    val errorInfo = HydraIngestionErrorInfo(err.ingestor, topic, err.error.getMessage,
      err.request.metadata, schema, err.request.payload).toJson.compactPrint

    AvroRecord(errorTopic, errorSchema, topic, errorInfo)
  }
}

case class HydraIngestionErrorInfo(ingestor: String,
                                   destination: Option[String],
                                   errorMessage: String,
                                   metadata: Map[String, String],
                                   schema: Option[String],
                                   payload: String)
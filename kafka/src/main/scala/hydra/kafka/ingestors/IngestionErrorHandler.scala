package hydra.kafka.ingestors

import akka.actor.Actor
import com.pluralsight.hydra.avro.JsonToAvroConversionException
import configs.syntax._
import hydra.avro.registry.ConfluentSchemaRegistry
import hydra.common.config.ConfigSupport
import hydra.core.avro.JsonToAvroConversionExceptionWithMetadata
import hydra.core.avro.schema.SchemaResourceLoader
import hydra.core.protocol.{HydraIngestionError, Produce}
import hydra.kafka.producer.AvroRecord
import spray.json.DefaultJsonProtocol

/**
  * A base error handler that sends a summary of ingestion errors  to a deadleter topic in Kafka.
  *
  * Created by alexsilva on 12/22/16.
  */
class IngestionErrorHandler extends Actor with ConfigSupport with DefaultJsonProtocol {

  private val errorTopic = applicationConfig.get[String]("ingest.error-topic").valueOrElse("__hydra_ingest_errors")

  lazy val kafkaTransport = context.actorSelection(applicationConfig.get[String](s"transports.kafka.path")
    .valueOrElse(s"/user/service/kafka_transport"))

  private lazy val errorSchema = {
    val name = applicationConfig.get[String]("ingest.error.schema").valueOrElse("exp.data.platform.IngestionError")
    val registry = ConfluentSchemaRegistry.forConfig(applicationConfig)
    val loader = new SchemaResourceLoader(registry.registryUrl, registry.registryClient)
    loader.getResource(name).schema
  }



  override def receive: Receive = {
    case error: HydraIngestionError => kafkaTransport ! Produce(buildPayload(error))
  }

  private def buildPayload(err: HydraIngestionError): AvroRecord = {
    val schema: Option[String] = err.error match {
      case e: JsonToAvroConversionException => Some(e.getSchema.toString)
      case e: JsonToAvroConversionExceptionWithMetadata => Some(e.res.location)
      case e: Exception => None

    }
    AvroRecord(errorTopic, errorSchema, None, "")
  }
}

case class HydraIngestionErrorInfo(err: HydraIngestionError, schema: Option[String])
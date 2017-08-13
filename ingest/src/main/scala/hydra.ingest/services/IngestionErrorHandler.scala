package hydra.ingest.services

import akka.actor.{Actor, ActorRef}
import com.pluralsight.hydra.avro.JsonToAvroConversionException
import configs.syntax._
import hydra.core.avro.JsonToAvroConversionExceptionWithMetadata
import hydra.core.avro.schema.{GenericSchemaResource, SchemaResource}
import hydra.core.ingest.{HydraRequest, RequestParams}
import hydra.core.notification.{HydraEvent, NotificationSupport}
import hydra.ingest.protocol.IngestionError
import hydra.ingest.services.IngestionErrorHandler.InvalidRequestError
import org.springframework.core.io.ByteArrayResource

/**
  * Created by alexsilva on 12/22/16.
  */
class IngestionErrorHandler extends Actor with NotificationSupport {

  val errorAvroSchema = applicationConfig.get[String]("ingest.error.schema")
    .valueOrElse("exp.engineering.platform.IngestionError")

  val errorTopicSufix = applicationConfig.get[String]("ingest.error_topic.suffix")
    .valueOrElse(".Error")

  override def receive: Receive = {
    case InvalidRequestError(ingestor, target, request, error) =>
      error match {
        case e: JsonToAvroConversionException => reportError(ingestor, request, e,
          Some(GenericSchemaResource(new ByteArrayResource(e.getSchema.toString(true).getBytes))))
        case e: JsonToAvroConversionExceptionWithMetadata => reportError(ingestor, request, e, Some(e.res))
        case e: Exception => reportError(ingestor, request, e, None)

      }

  }

  private def reportError(ingestor: ActorRef, request: HydraRequest, e: Throwable, schema: Option[SchemaResource]) = {
    val errorTopic = request.correlationId + errorTopicSufix
    val errorMsg = IngestionError(ingestor.path.toString, System.currentTimeMillis(), errorTopic, request.payload,
      schema.map(_.location), e.getClass.getSimpleName, e.getMessage)
    val errorRequest = HydraRequest(1, request.payload)
      .withMetadata(RequestParams.HYDRA_SCHEMA_PARAM -> errorAvroSchema)
      .withMetadata(RequestParams.HYDRA_KAFKA_TOPIC_PARAM -> errorTopic)
    observers ! errorMsg
  }
}

object IngestionErrorHandler {

  case class InvalidRequestError(source: ActorRef, target: Option[String], request: HydraRequest,
                                 error: Throwable) extends HydraEvent[ActorRef]

}


package hydra.ingest.services

import akka.actor.{Actor, ActorRef}
import com.pluralsight.hydra.avro.JsonToAvroConversionException
import configs.syntax._
import hydra.core.avro.JsonToAvroConversionExceptionWithMetadata
import hydra.core.avro.schema.{GenericSchemaResource, SchemaResource}
import hydra.core.ingest.{HydraRequest, IngestionParams}
import hydra.core.notification.NotificationSupport
import hydra.ingest.protocol.IngestionError
import hydra.ingest.services.IngestionErrorHandler.HandleError
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
    case HandleError(ingestor, request, error) => {
      error match {
        case e: JsonToAvroConversionException => reportError(ingestor, request, e,
          Some(GenericSchemaResource(new ByteArrayResource(e.getSchema.toString(true).getBytes))))
        case e: JsonToAvroConversionExceptionWithMetadata => reportError(ingestor, request, e, Some(e.res))
        case e: Exception => reportError(ingestor, request, e, None)

      }
    }
  }

  private def reportError(ingestor: ActorRef, request: HydraRequest, e: Throwable, schema: Option[SchemaResource]) = {
    val errorTopic = request.label + errorTopicSufix
    val errorMsg = IngestionError(ingestor.path.toString, System.currentTimeMillis(), errorTopic, request.payload,
      schema.map(_.location), e.getClass.getSimpleName, e.getMessage)
    val errorRequest = HydraRequest(errorTopic, request.payload)
      .withMetadata(IngestionParams.HYDRA_SCHEMA_PARAM -> errorAvroSchema)
    observers ! errorMsg
  }
}

object IngestionErrorHandler {

  case class HandleError(ingestor: ActorRef, request: HydraRequest, error: Throwable)

}


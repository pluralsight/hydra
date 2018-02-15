package hydra.kafka.producer

import akka.actor.ActorRef
import hydra.core.ingest.{HydraRequest, RequestParams}
import hydra.core.ingest.RequestParams.HYDRA_RECORD_FORMAT_PARAM
import hydra.core.transport.{HydraRecord, RecordFactory}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/**
  * A wrapper around the RecordFactory implementations for Kafka.
  *
  * Created by alexsilva on 2/23/17.
  */
class KafkaRecordFactories(schemaLoader: ActorRef) extends RecordFactory[Any, Any] {

  private val avroRecordFactory = new AvroRecordFactory(schemaLoader)

  def factoryFor(request: HydraRequest): Try[KafkaRecordFactory[_, _]] = {
    deleteOrElse(request) {
      request.metadataValue(HYDRA_RECORD_FORMAT_PARAM) match {
        case Some(value) if (value.equalsIgnoreCase("string")) => StringRecordFactory
        case Some(value) if (value.equalsIgnoreCase("json")) => JsonRecordFactory
        case Some(value) if (value.equalsIgnoreCase("avro")) => avroRecordFactory
        case Some(value) => throw new IllegalArgumentException(s"'$value' is not a valid format.")
        case _ => avroRecordFactory
      }
    }
  }

  private def deleteOrElse(r: HydraRequest)
                          (orElse: => KafkaRecordFactory[_, _]): Try[KafkaRecordFactory[_, _]] = {
    Try {
      r.metadataValue(RequestParams.HYDRA_DELETE_DIRECTIVE)
        .map(_ => DeleteTombstoneRecordFactory)
        .getOrElse(orElse)
    }
  }

  override def build(request: HydraRequest)
                    (implicit ec: ExecutionContext): Future[HydraRecord[_, _]] = {
    factoryFor(request) match {
      case Success(factory) => factory.build(request)
      case Failure(ex) => Future.failed(ex)
    }
  }
}



package hydra.kafka.producer

import akka.actor.ActorRef
import hydra.core.ingest.HydraRequest
import hydra.core.ingest.RequestParams.HYDRA_RECORD_FORMAT_PARAM
import hydra.core.transport.{HydraRecord, RecordFactory}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/**
  * A wrapper around the RecordFactory implementations for Kafka.
  *
  * Created by alexsilva on 2/23/17.
  */
class KafkaRecordFactories(schemaLoader:ActorRef) extends RecordFactory[Any, Any] {

  private val avroRecordFactory = new AvroRecordFactory(schemaLoader)

  def factoryFor(request: HydraRequest): Try[KafkaRecordFactory[_, _]] = {
    request.metadataValue(HYDRA_RECORD_FORMAT_PARAM) match {
      case Some(value) if (value.equalsIgnoreCase("string")) => Success(StringRecordFactory)
      case Some(value) if (value.equalsIgnoreCase("json")) => Success(JsonRecordFactory)
      case Some(value) if (value.equalsIgnoreCase("avro")) => Success(avroRecordFactory)
      case Some(value) => Failure(new IllegalArgumentException(s"'$value' is not a valid format."))
      case _ => Success(avroRecordFactory)
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



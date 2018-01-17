package hydra.kafka.producer

import hydra.core.ingest.HydraRequest
import hydra.core.ingest.RequestParams.HYDRA_RECORD_FORMAT_PARAM
import hydra.core.transport.{HydraRecord, RecordFactory}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/**
  * A type class wrapper around the RecordFactory implementations for Kafka.
  *
  * Created by alexsilva on 2/23/17.
  */
object KafkaRecordFactories extends RecordFactory[Any, Any] {

  def factoryFor(request: HydraRequest): Try[KafkaRecordFactory[_, _]] = {
    request.metadataValue(HYDRA_RECORD_FORMAT_PARAM) match {
      case Some(value) if (value.equalsIgnoreCase("string")) => Success(StringRecordFactory)
      case Some(value) if (value.equalsIgnoreCase("json")) => Success(JsonRecordFactory)
      case Some(value) if (value.equalsIgnoreCase("avro")) => Success(AvroRecordFactory)
      case Some(value) => Failure(new IllegalArgumentException(s"'$value' is not a valid format."))
      case _ => Success(AvroRecordFactory)
    }
  }

  override def build(request: HydraRequest)
                    (implicit ec: ExecutionContext): Future[HydraRecord[_, _]] = {
    factoryFor(request).map(_.build(request)).get
  }
}



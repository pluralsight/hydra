package hydra.jdbc

import hydra.core.ingest.RequestParams._
import hydra.core.ingest.{HydraRequest, Ingestor, TransportOps}
import hydra.core.protocol._
import hydra.jdbc.transport.JdbcRecordFactory

/**
  * Created by alexsilva on 5/19/17.
  */
class JdbcIngestor extends Ingestor with TransportOps {

  override val transportName = "jdbc_transport"

  implicit val recordFactory = JdbcRecordFactory

  ingest {
    case Publish(request) =>
      sender ! request.metadataValue("hydra-jdbc-target").map(_ => Join).getOrElse(Ignore)

    case Validate(request) =>
      val isValid = getSchema(request).map(_ => ValidRequest)
        .getOrElse(InvalidRequest(new IllegalArgumentException("A schema is required.")))

      sender ! isValid

    case Ingest(request) =>
      sender ! transport(request)
  }

  private def getSchema(request: HydraRequest): Option[String] =
    request.metadataValue(HYDRA_SCHEMA_PARAM).orElse(request.metadataValue(HYDRA_KAFKA_TOPIC_PARAM))
}

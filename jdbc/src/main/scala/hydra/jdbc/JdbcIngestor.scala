package hydra.jdbc

import com.typesafe.config.Config
import hydra.core.ingest.{Ingestor, TransportOps}
import hydra.core.protocol._
import configs.syntax._

class JdbcIngestor extends Ingestor with TransportOps {
  override def recordFactory = JdbcRecordFactory

  ingest {
    case Publish(request) =>
      sender ! (if (request.metadataValue(JdbcRecordFactory.DB_PROFILE_PARAM).isDefined) Join else Ignore)

    case Validate(request) =>
      val validation = request.metadataValue(JdbcRecordFactory.DB_PROFILE_PARAM) match {
        case None => InvalidRequest(new IllegalArgumentException("A db profile name must be supplied."))
        case Some(profile) =>
          applicationConfig.get[Config](s"transports.jdbc.profiles.$profile").map(_ => validate(request))
            .valueOrElse(InvalidRequest(new IllegalArgumentException(s"No db profile named '$profile' found.")))
      }

      sender ! validation

    case Ingest(record, supervisor, ackStrategy) => transport(record, supervisor, ackStrategy)
  }

  override def transportName = "jdbc"
}

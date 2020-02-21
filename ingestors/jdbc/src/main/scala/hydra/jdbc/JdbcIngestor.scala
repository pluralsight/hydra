package hydra.jdbc

import com.typesafe.config.Config
import configs.syntax._
import hydra.core.akka.SchemaRegistryActor
import hydra.core.ingest.{HydraRequest, Ingestor, TransportOps}
import hydra.core.protocol._

import scala.util.Try

class JdbcIngestor extends Ingestor with TransportOps {

  private val schemaRegistryActor =
    context.actorOf(SchemaRegistryActor.props(applicationConfig))

  override def recordFactory = new JdbcRecordFactory(schemaRegistryActor)

  override def validateRequest(request: HydraRequest): Try[HydraRequest] = {
    Try {
      val profile =
        request.metadataValue(JdbcRecordFactory.DB_PROFILE_PARAM).get
      applicationConfig
        .get[Config](s"transports.jdbc.profiles.$profile")
        .map(_ => request)
        .valueOrThrow(_ =>
          new IllegalArgumentException(s"No db profile named '$profile' found.")
        )
    }
  }

  ingest {
    case Publish(request) =>
      sender ! (if (request.hasMetadata(JdbcRecordFactory.DB_PROFILE_PARAM))
                  Join
                else Ignore)

    case Ingest(record, ackStrategy) => transport(record, ackStrategy)
  }

  override def transportName = "jdbc"
}

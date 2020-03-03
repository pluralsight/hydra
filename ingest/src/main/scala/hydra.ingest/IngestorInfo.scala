package hydra.ingest

import akka.actor.ActorPath
import hydra.core.protocol.HydraMessage
import org.joda.time.DateTime

/**
  *
  * @param name         The name of the ingestor.
  * @param path         The akka path where this ingestor/actor resides.
  * @param group         The group to which this ingestor belongs to.
  *                     Ingestors belonging to the 'global' pool are included in all Publish messages.
  * @param registeredAt Timestamp at which this ingestor was registered.
  */
case class IngestorInfo(
    name: String,
    group: String,
    path: ActorPath,
    registeredAt: DateTime
) extends HydraMessage

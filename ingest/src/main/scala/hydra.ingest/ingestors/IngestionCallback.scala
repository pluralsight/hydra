package hydra.ingest.ingestors

import akka.actor.ActorSystem
import hydra.core.ingest.IngestionReport
import hydra.core.protocol.{HydraApplicationError, HydraError}

trait IngestionCallback {
  /**
    * Will be called when the ingest protocol has finished.
    */
  def onCompletion(report: IngestionReport): Unit

  /**
    * Reserved for unrecoverable errors.
    * @param system
    * @param error
    */
  def onError(system: ActorSystem, error: HydraError): Unit = {
    system.eventStream.publish(HydraApplicationError(error.cause))
  }
}

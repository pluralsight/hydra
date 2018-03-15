package hydra.core.connect

import akka.actor.Actor
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.Publish
import com.typesafe.config.Config
import hydra.common.logging.LoggingAdapter
import hydra.core.Settings
import hydra.core.ingest.{HydraRequest, IngestionReport}
import hydra.core.protocol.{HydraMessage, IngestorCompleted, InitiateRequest}

trait Connector extends Actor with LoggingAdapter {

  def id: String

  def config: Config

  val settings = new ConnectorSettings(config, context.system)

  protected val publisher = DistributedPubSub(context.system).mediator

  def init(): Unit = {}

  def stop(): Unit = {}


  override def preStart(): Unit = {
    super.preStart()
    init()
  }

  override def postStop(): Unit = {
    super.postStop()
    stop()
  }

  override def receive: Receive = {
    case req: HydraRequest => publisher ! publishRequest(req)

    case r: IngestionReport if r.statusCode != 200 => onIngestionError(r)
  }

  protected def publishRequest(request: HydraRequest): Publish = {
    Publish(Settings.IngestTopicName, InitiateRequest(request, settings.requestTimeout), true)
  }

  protected def onIngestionError(r: IngestionReport) = {
    r.ingestors.foreach {
      case (name, status) if status != IngestorCompleted =>
        log.error(f"Received failed response for request ${r.correlationId}:" +
          f"\n\tStatus code -> ${r.statusCode}, Cause -> ${status.message}")

        context.system.eventStream
          .publish(HydraConnectIngestError(id, name, status.statusCode.intValue(), status.message))
    }
  }
}

case class RequestReceived(request: HydraRequest) extends HydraMessage

case class RequestConfirmed(deliveryId: Long) extends HydraMessage

case class HydraConnectIngestError(connectorId: String, source: String,
                                   statusCode: Int, msg: String,
                                   metadata: Map[String, String] = Map.empty) extends HydraMessage
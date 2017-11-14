package hydra.core.transport

import hydra.core.akka.InitializingActor
import hydra.core.akka.InitializingActor.InitializationError
import hydra.core.protocol._

import scala.concurrent.duration.{FiniteDuration, _}

/**
  * Created by alexsilva on 12/1/15.
  */

trait Transport extends InitializingActor {

  override def initTimeout: FiniteDuration = 2.seconds

  override val baseReceive: Receive = {
    case Produce(r, sup, _, deliveryId) =>
      log.info(s"Produce message was not handled by ${thisActorName}.")
      val err = new IllegalStateException("Transport did not implement Produce; message will not be produced.")
      sender ! RecordNotProduced(deliveryId, r, err, sup)

    case r@RecordProduced(_, _) =>
      log.info(s"$thisActorName: Record produced.")
      sender ! r

    case r@RecordNotProduced(_, _, error, _) =>
      log.error(s"$thisActorName: $error")
      sender ! r
  }

  def transport(next: Receive) = compose(next)


  override def initializationError(ex: Throwable): Receive = {
    //todo: customize this error
    case _ => sender ! InitializationError(ex)
  }
}

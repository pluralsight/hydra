package hydra.core.transport

/**
  * Defines a Retry Strategy for messages being sent through Hydra.
  *
  * The strategies are only relevant when there is an error communicating to the underlying data store.
  *
  * Created by alexsilva on 10/4/16.
  */
trait DeliveryStrategy {
  def retryOnFailure: Boolean
}

object DeliveryStrategy {

  def apply(strategy: String): DeliveryStrategy = {
    Option(strategy).map(_.trim.toLowerCase) match {
      case Some("at-least-once") => AtLeastOnce
      case _ => AtMostOnce
    }
  }

  case object AtMostOnce extends DeliveryStrategy {
    override val retryOnFailure: Boolean = false

  }

  case object AtLeastOnce extends DeliveryStrategy {
    override val retryOnFailure: Boolean = true
  }

}


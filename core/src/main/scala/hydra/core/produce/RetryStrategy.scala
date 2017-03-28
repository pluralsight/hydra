package hydra.core.produce

/**
  * Defines a Retry Strategy for messages being sent through Hydra.
  *
  * The strategies are only relevant when there is an error communicating to the underlying data store.
  *
  * Created by alexsilva on 10/4/16.
  */
trait RetryStrategy {
  def retryOnFailure: Boolean
}

object RetryStrategy {

  def apply(strategy: String): RetryStrategy = {
    strategy match {
      case "retry" => Retry
      case _ => Fail
    }
  }

  case object Fail extends RetryStrategy {
    override val retryOnFailure: Boolean = false

  }

  case object Retry extends RetryStrategy {
    override val retryOnFailure: Boolean = true
  }

}


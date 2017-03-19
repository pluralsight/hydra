package hydra.core.ingest

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
      case "until-success" => UntilSuccess
      case _ => Fail
    }
  }

  case object Fail extends RetryStrategy {
    override val retryOnFailure: Boolean = false

  }

  case object UntilSuccess extends RetryStrategy {
    override val retryOnFailure: Boolean = true
  }

}


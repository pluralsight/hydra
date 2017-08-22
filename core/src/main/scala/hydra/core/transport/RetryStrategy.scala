package hydra.core.transport

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
    Option(strategy).map(_.trim.toLowerCase) match {
      case Some("persist") => Persist
      case _ => Ignore
    }
  }

  case object Ignore extends RetryStrategy {
    override val retryOnFailure: Boolean = false

  }

  case object Persist extends RetryStrategy {
    override val retryOnFailure: Boolean = true
  }

}


package hydra.core.produce

import hydra.core.ingest.{HydraRequest, RetryStrategy}

/**
  * Created by alexsilva on 10/30/15.
  *
  * Generic trait that defines a record that is to be persisted by Hydra.
  *
  * @tparam K The record key type
  * @tparam P The record payload type
  */
trait HydraRecord[+K, +P] {

  def destination:String

  def key: Option[K]

  def payload: P

  /**
    * The validation strategy to apply to this message. Defaults to Strict.
    *
    * See [[ValidationStrategy]].
    *
    * @return
    */
  def validationStrategy: ValidationStrategy = ValidationStrategy.Strict

  /**
    * Whether or not to retry the message in case of a failure.
    * The actual implementation is up to the transport. but it should use some type of backing off strategy
    * with eventually giving up.
    *
    * @return
    */
  def retryStrategy: RetryStrategy
}
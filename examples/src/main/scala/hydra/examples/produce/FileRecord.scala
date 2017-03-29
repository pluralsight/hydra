package hydra.examples.produce

import hydra.core.produce.{HydraRecord, RetryStrategy}

/**
  * Created by alexsilva on 3/29/17.
  */
case class FileRecord(destination: String, payload: String) extends HydraRecord[String, String] {

  override val key: Option[String] = None

  override val retryStrategy: RetryStrategy = RetryStrategy.Fail
}

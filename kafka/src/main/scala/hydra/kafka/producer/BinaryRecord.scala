package hydra.kafka.producer

import hydra.core.transport.RetryStrategy
import hydra.core.transport.RetryStrategy.Ignore

/**
  * Created by alexsilva on 11/30/15.
  */
case class BinaryRecord(destination: String, key: Option[String], payload: Array[Byte],
                        retryStrategy: RetryStrategy = Ignore) extends KafkaRecord[String, Array[Byte]]

package hydra.kafka.producer

import hydra.core.produce.RetryStrategy
import hydra.core.produce.RetryStrategy.Fail

/**
  * Created by alexsilva on 11/30/15.
  */
case class BinaryRecord(destination: String, key: Option[String], payload: Array[Byte],
                        retryStrategy: RetryStrategy = Fail) extends KafkaRecord[String, Array[Byte]]

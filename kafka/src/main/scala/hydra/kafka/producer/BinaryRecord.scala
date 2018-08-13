package hydra.kafka.producer

import hydra.core.transport.AckStrategy

/**
  * Created by alexsilva on 11/30/15.
  */
case class BinaryRecord(destination: String, key: Option[String], payload: Array[Byte],
                        ackStrategy: AckStrategy)
  extends KafkaRecord[String, Array[Byte]]

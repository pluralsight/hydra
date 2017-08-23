package hydra.kafka.producer

import hydra.core.transport.DeliveryStrategy
import hydra.core.transport.DeliveryStrategy.BestEffort

/**
  * Created by alexsilva on 11/30/15.
  */
case class BinaryRecord(destination: String, key: Option[String], payload: Array[Byte],
                        deliveryStrategy: DeliveryStrategy = BestEffort) extends KafkaRecord[String, Array[Byte]]

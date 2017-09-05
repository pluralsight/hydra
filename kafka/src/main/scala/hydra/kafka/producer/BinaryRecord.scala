package hydra.kafka.producer

import hydra.core.transport.DeliveryStrategy
import hydra.core.transport.DeliveryStrategy.AtMostOnce

/**
  * Created by alexsilva on 11/30/15.
  */
case class BinaryRecord(destination: String, key: Option[String], payload: Array[Byte],
                        deliveryStrategy: DeliveryStrategy = AtMostOnce) extends KafkaRecord[String, Array[Byte]]

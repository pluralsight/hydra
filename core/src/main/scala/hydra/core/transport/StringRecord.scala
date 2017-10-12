package hydra.core.transport

import hydra.core.transport.DeliveryStrategy.AtMostOnce

case class StringRecord(destination: String, key: Option[String], payload: String,
                        deliveryStrategy: DeliveryStrategy = AtMostOnce,
                        ackStrategy: AckStrategy = AckStrategy.None) extends HydraRecord[String, String]
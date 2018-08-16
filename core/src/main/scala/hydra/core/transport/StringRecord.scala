package hydra.core.transport


case class StringRecord(destination: String, key: Option[String], payload: String, ackStrategy: AckStrategy) extends HydraRecord[String, String]
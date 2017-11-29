package hydra.core.transport


case class StringRecord(destination: String, key: Option[String], payload: String) extends HydraRecord[String, String]
package hydra.kafka.producer

/**
  * Created by alexsilva on 11/30/15.
  */
case class BinaryRecord(destination: String, key: Option[String], payload: Array[Byte])
  extends KafkaRecord[String, Array[Byte]]

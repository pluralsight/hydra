package hydra.avro.io

import org.apache.avro.generic.GenericRecord

import scala.collection.JavaConverters._

sealed trait Operation

case class Upsert(record: GenericRecord) extends Operation

case class DeleteByKey(keys: Map[String, AnyRef]) extends Operation {
  def this(jmap: java.util.Map[String, AnyRef]) = this(jmap.asScala.toMap)
}

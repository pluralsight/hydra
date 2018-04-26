package hydra.avro.io

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

import scala.collection.JavaConverters._

sealed trait Operation

case class Upsert(record: GenericRecord) extends Operation

case class DeleteByKey(keys: Map[Schema.Field, AnyRef]) extends Operation {
  def this(jmap: java.util.Map[Schema.Field, AnyRef]) = this(jmap.asScala.toMap)
}
/*
 * Copyright (C) 2017 Pluralsight, LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package hydra.avro.util

import com.pluralsight.hydra.avro.JsonToAvroConversionException
import hydra.avro.registry.JsonToAvroConversionExceptionWithMetadata
import hydra.avro.resource.SchemaResource
import org.apache.avro.Schema
import org.apache.avro.Schema.Field

import scala.collection.mutable

/**
  * Created by alexsilva on 12/7/15.
  */
object AvroUtils {

  import scala.collection.JavaConverters._

  val pattern = "^(?!\\d|[a-zA-Z]|_)".r

  private[avro] val SEEN_EQUALS = new ThreadLocal[mutable.Set[SeenPair]]() {
    override protected def initialValue = new mutable.HashSet[SeenPair]()
  }

  private[avro] val NO_HASHCODE = Integer.MIN_VALUE

  /**
    * Valid fields in Avro need to start with a number, an underscore or a letter.  This function checks the
    * field name and replaces the first character with an underscore if it is not valid.
    *
    * @param name
    */
  def cleanName(name: String) = {
    pattern findFirstIn name match {
      case Some(str) => "_" + name.substring(1)
      case None      => name
    }
  }

  def getField(name: String, schema: Schema): Field = {
    Option(schema.getField(name))
      .getOrElse(
        throw new IllegalArgumentException(s"Field $name is not in schema.")
      )
  }

  /**
    * Returns the primary keys (if any) defined for that schema.
    *
    * Primary keys are defined by adding a property named "key" to the avro record,
    * which can contain a single field name
    * or a comma delimmited list of field names (for composite primary keys.)
    *
    * @param schema
    * @return An empty sequence if no primary key(s) are defined.
    */
  def getPrimaryKeys(schema: Schema): Seq[Field] = {
    Option(schema.getProp("hydra.key")).map(_.split(",")) match {
      case Some(ids) => ids.map(getField(_, schema))
      case None      => Seq.empty
    }
  }

  /**
    * A "ligher" equals that looks a fields and names primarily.
    * Similar to record schema.compare, but without taking properties into consideration, since
    * we are using properties for primary keys.
    *
    * @param one
    * @param other
    */
  def areEqual(one: Schema, other: Schema): Boolean = {
    val seen = SEEN_EQUALS.get
    val here = SeenPair(one.hashCode(), other.hashCode())
    val equals = {
      if (seen.contains(here)) return true
      if (one eq other) return true
      if (one.getFullName != other.getFullName) return false
      one.getFields.asScala.map(_.name()).toSet == other.getFields.asScala
        .map(_.name())
        .toSet
    }

    if (equals) seen.add(here)

    equals
  }

  def improveException(ex: Throwable, schema: SchemaResource, registryUrl:String) = {
    ex match {
      case e: JsonToAvroConversionException =>
        JsonToAvroConversionExceptionWithMetadata(e, schema, registryUrl)
      case e: Exception => e
    }
  }

  private[avro] case class SeenPair private (s1: Int, s2: Int) {

    override def equals(o: Any): Boolean =
      (this.s1 == o.asInstanceOf[SeenPair].s1) && (this.s2 == o
        .asInstanceOf[SeenPair]
        .s2)

    override def hashCode: Int = s1 + s2
  }

}

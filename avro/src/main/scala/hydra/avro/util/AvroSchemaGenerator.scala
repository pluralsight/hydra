package hydra.avro.util

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

import java.util.Random

import com.fasterxml.jackson.databind.node.{ArrayNode, JsonNodeType}
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}

import scala.collection.JavaConverters._

/**
  * Simple utility class that takes in a Json object and creates an Avro schema from that sample.
  */
class AvroSchemaGenerator {

  import AvroSchemaGenerator._

  val mapper = new ObjectMapper()

  def convert(json: String, namespace: String, name: String): String = {
    val jsonNode = mapper.readTree(json)
    val finalSchema = mapper.createObjectNode()
    finalSchema.put("namespace", namespace)
    finalSchema.put(NAME, name)
    finalSchema.put(TYPE, RECORD)
    finalSchema.set(FIELDS, getFields(jsonNode))
    mapper.writerWithDefaultPrettyPrinter().writeValueAsString(finalSchema)
  }

  private def getFields(jsonNode: JsonNode): ArrayNode = {
    val fields = mapper.createArrayNode()
    jsonNode.fields().asScala.foreach { field =>
      val node = field.getValue
      node.getNodeType match {
        case JsonNodeType.NUMBER =>
          fields.add(
            mapper
              .createObjectNode()
              .put(NAME, field.getKey)
              .put(
                TYPE,
                if (node.isLong) "long"
                else "double"
              )
          )
        case JsonNodeType.STRING =>
          fields.add(
            mapper
              .createObjectNode()
              .put(NAME, field.getKey())
              .put(TYPE, STRING)
          )
        case JsonNodeType.OBJECT =>
          val objNode = mapper.createObjectNode()
          objNode.put(NAME, field.getKey())
          objNode.set(
            TYPE,
            mapper
              .createObjectNode()
              .put(TYPE, RECORD)
              .put(NAME, generateRandomKey(field))
              .set(FIELDS, getFields(node))
          )
          fields.add(objNode)
        case JsonNodeType.BOOLEAN =>
          fields.add(
            mapper
              .createObjectNode()
              .put(NAME, field.getKey())
              .put(TYPE, BOOLEAN)
          )
        case JsonNodeType.ARRAY =>
          val arrayNode = node.asInstanceOf[ArrayNode]
          val element = arrayNode.get(0)
          val objectNode = mapper.createObjectNode()
          objectNode.put(NAME, field.getKey())
          if (element.getNodeType() == JsonNodeType.NUMBER) {
            objectNode.set(
              TYPE,
              mapper
                .createObjectNode()
                .put(TYPE, ARRAY)
                .put(
                  ITEMS,
                  if (node.isLong) "long"
                  else "double"
                )
            )
            fields.add(objectNode)
          } else if (element.getNodeType() == JsonNodeType.STRING) {
            objectNode.set(
              TYPE,
              mapper.createObjectNode().put(TYPE, ARRAY).put(ITEMS, STRING)
            )
            fields.add(objectNode)
          } else {
            objectNode.set(
              TYPE,
              mapper
                .createObjectNode()
                .put(TYPE, ARRAY)
                .set(
                  ITEMS,
                  mapper
                    .createObjectNode()
                    .put(TYPE, RECORD)
                    .put(NAME, generateRandomKey(field))
                    .set(FIELDS, getFields(element))
                )
            )
          }
          fields.add(objectNode)
        case _ =>
          throw new IllegalArgumentException(
            s"Unable to determine action for node ${node.getNodeType}. " +
              "Allowed types are ARRAY, STRING, NUMBER, OBJECT"
          )
      }
    }

    fields
  }

  private def generateRandomKey(entry: java.util.Map.Entry[String, JsonNode]) =
    entry.getKey() + "_" + new Random().nextInt(100)

}

object AvroSchemaGenerator {
  val NAME = "name"
  val BOOLEAN = "boolean"
  val TYPE = "type"
  val ARRAY = "array"
  val ITEMS = "items"
  val STRING = "string"
  val RECORD = "record"
  val FIELDS = "fields"
}

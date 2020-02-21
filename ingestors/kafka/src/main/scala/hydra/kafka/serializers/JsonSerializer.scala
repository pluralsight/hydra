/*
 * Copyright (C) 2016 Pluralsight, LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package hydra.kafka.serializers

import java.io.ByteArrayInputStream
import java.util

import com.fasterxml.jackson.databind.{
  JsonNode,
  ObjectMapper,
  SerializationFeature
}
import hydra.common.config.ConfigSupport
import org.apache.kafka.common.serialization._

/**
  * Created by alexsilva on 12/1/15.
  */
class JsonSerializer extends Serializer[JsonNode] with ConfigSupport {

  import JsonSerializer._

  override def serialize(topic: String, data: JsonNode): Array[Byte] = {
    mapper.writeValueAsBytes(data)
  }

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {
    val indent = Option(configs.get("kafka.encoders.json.indent.output"))
      .map(_.toString.toBoolean)
      .getOrElse(false)
    mapper.configure(SerializationFeature.INDENT_OUTPUT, indent)
  }

  override def close(): Unit = {
    //nothing to do
  }
}

object JsonSerializer {
  val mapper = new ObjectMapper
}

class JsonDeserializer extends Deserializer[JsonNode] {

  import JsonDeserializer._

  override def deserialize(topic: String, bytes: Array[Byte]): JsonNode = {
    mapper.readTree(new ByteArrayInputStream(bytes))
  }

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {
    val indent = Option(configs.get("kafka.encoders.json.indent.output"))
      .map(_.toString.toBoolean)
      .getOrElse(false)
    mapper.configure(SerializationFeature.INDENT_OUTPUT, indent)
  }

  override def close(): Unit = {
    //nothing
  }
}

object JsonDeserializer {
  val mapper = new ObjectMapper
}

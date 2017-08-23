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

package hydra.ingest.protocol

import hydra.avro.registry.{ConfluentSchemaRegistry, RegistrySchemaResource}
import hydra.common.config.ConfigSupport
import hydra.core.protocol.HydraMessage
import org.apache.avro.Schema
import spray.json.{DefaultJsonProtocol, JsNumber, JsObject, JsString, JsValue, JsonFormat}

import scala.util.Try

/**
  * Created by alexsilva on 12/23/16.
  */

case class IngestionError(source: String, timestamp: Long,
                          destinationTopic: String, payload: String, schema: Option[String], errorType: String,
                          errorMessage: String) extends HydraMessage

object IngestionError extends DefaultJsonProtocol with ConfigSupport {

  private val registry = ConfluentSchemaRegistry.forConfig(applicationConfig)

  implicit val ingestionErrorFormat = jsonFormat7(IngestionError.apply)

  implicit val schemaResourceFormat = new JsonFormat[RegistrySchemaResource] {

    private def locationToId(location: String) = Try(location.substring(location.lastIndexOf("/")).toInt)

    override def read(json: JsValue): RegistrySchemaResource = {
      val jsObject = json.asJsObject
      jsObject.getFields("registry", "subject", "id", "version", "location") match {
        case Seq(jregistry, subject, id, version, location) =>
          val registryUrl = jregistry.convertTo[String]
          val locationStr = location.convertTo[String]
          val schema = locationToId(locationStr)
            .map(id => registry.registryClient.getByID(id))
            .getOrElse(Schema.create(Schema.Type.RECORD))
          RegistrySchemaResource(registryUrl, subject.convertTo[String], id.convertTo[Int],
            version.convertTo[Int], schema)
        case other => throw new IllegalArgumentException(s"Cannot deserialize schema: invalid input. Raw input: $other")
      }
    }

    override def write(obj: RegistrySchemaResource): JsValue = {
      JsObject(
        Map(
          "registry" -> JsString(obj.registryUrl),
          "subject" -> JsString(obj.subject),
          "id" -> JsNumber(obj.id),
          "version" -> JsNumber(obj.id),
          "location" -> JsString(obj.location)
        )
      )
    }
  }

}


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

package hydra.kafka.producer

import com.pluralsight.hydra.avro.JsonConverter
import hydra.avro.registry.ConfluentSchemaRegistry
import hydra.avro.resource.{SchemaResource, SchemaResourceLoader}
import hydra.avro.util.AvroUtils
import hydra.common.config.ConfigSupport
import hydra.core.ingest.{HydraRequest, RequestParams}
import hydra.core.transport.ValidationStrategy.Strict
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

import scala.util.Try

/**
  * Created by alexsilva on 1/11/17.
  */
object AvroRecordFactory extends KafkaRecordFactory[String, GenericRecord] with ConfigSupport {

  val schemaRegistry = ConfluentSchemaRegistry.forConfig(applicationConfig)

  lazy val schemaResourceLoader = new SchemaResourceLoader(schemaRegistry.registryUrl, schemaRegistry.registryClient)


  override def build(request: HydraRequest): Try[AvroRecord] = {
    val schemaResource: Try[SchemaResource] = Try(schemaResourceLoader.getResource(getSubject(request)))
    schemaResource.flatMap { s =>
      val strict = request.validationStrategy == Strict
      val converter = new JsonConverter[GenericRecord](s.schema, strict)
      Try(converter.convert(request.payload))
        .map(rec => buildRecord(request, rec, s.schema))
        .recover { case ex => throw schemaResource.map(r => AvroUtils.improveException(ex, r)).getOrElse(ex) }
    }
  }

  private def buildRecord(request: HydraRequest, rec: GenericRecord, schema: Schema) = {
    AvroRecord(getTopic(request), schema, getKey(request), rec)
  }

  def getSubject(request: HydraRequest): String = {
    request.metadataValue(RequestParams.HYDRA_SCHEMA_PARAM).getOrElse(getTopic(request))
  }

}





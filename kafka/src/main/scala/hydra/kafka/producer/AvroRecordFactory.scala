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

import hydra.avro.registry.ConfluentSchemaRegistry
import hydra.common.config.ConfigSupport
import hydra.core.avro.AvroValidation
import hydra.core.avro.schema.{SchemaResource, SchemaResourceLoader}
import hydra.core.ingest.{HydraRequest, RequestParams}
import org.apache.avro.generic.GenericRecord

import scala.util.Try

/**
  * Created by alexsilva on 1/11/17.
  */
object AvroRecordFactory extends KafkaRecordFactory[String, GenericRecord] with ConfigSupport
  with ConfluentSchemaRegistry with AvroValidation {

  override val config = applicationConfig

  lazy val schemaResourceLoader = new SchemaResourceLoader(registryUrl, registryClient)

  override def build(request: HydraRequest): Try[AvroRecord] = {
    val schemaResource: Try[SchemaResource] = Try(schemaResourceLoader.getResource(getSubject(request)))
    Try(AvroRecord(getTopic(request), schemaResource.get.schema, getKey(request),
      request.payload, request.retryStrategy))
  }

  def getSubject(request: HydraRequest): String = {
    request.metadataValue(RequestParams.HYDRA_SCHEMA_PARAM).getOrElse(getTopic(request))
  }

}





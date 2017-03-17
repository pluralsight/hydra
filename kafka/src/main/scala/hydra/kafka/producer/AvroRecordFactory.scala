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

import com.pluralsight.hydra.avro.{JsonConverter, JsonToAvroConversionException}
import hydra.common.config.ConfigSupport
import hydra.core.avro.JsonToAvroConversionExceptionWithMetadata
import hydra.core.avro.registry.ConfluentSchemaRegistry
import hydra.core.avro.schema.{SchemaResource, SchemaResourceLoader}
import hydra.core.ingest.{HydraRequest, IngestionParams}
import hydra.core.producer.ValidationStrategy.Strict
import hydra.core.protocol.{InvalidRequest, MessageValidationResult, ValidRequest}
import org.apache.avro.generic.GenericRecord

import scala.util.{Failure, Success, Try}

/**
  * Created by alexsilva on 1/11/17.
  */
object AvroRecordFactory extends KafkaRecordFactory[String, GenericRecord] with ConfigSupport
  with ConfluentSchemaRegistry {

  lazy val schemaResourceLoader = new SchemaResourceLoader(registryUrl, registry)

  override def build(request: HydraRequest): AvroRecord = {
    val schemaResource: Try[SchemaResource] = Try(schemaResourceLoader.getResource(getSubject(request)))
    AvroRecord(getTopic(request), schemaResource.get.schema, getKey(request), request.payload, request.retryStrategy)
  }

  override def validate(request: HydraRequest): MessageValidationResult = {

    val schemaResource: Try[SchemaResource] = Try(schemaResourceLoader.getResource(getSubject(request)))

    val record = schemaResource.map { s =>
      val strict = request.validationStrategy == Strict
      val converter = new JsonConverter[GenericRecord](s.schema, strict)
      converter.convert(request.payload)
    }

    record match {
      case Success(c) => ValidRequest
      case Failure(ex) => InvalidRequest(schemaResource.map(r => improveException(ex, r)).getOrElse(ex))
    }
  }

  private def improveException(ex: Throwable, schemaResource: SchemaResource) = {
    ex match {
      case e: JsonToAvroConversionException => JsonToAvroConversionExceptionWithMetadata(e, schemaResource)
      case e: Exception => e
    }
  }

  def getSubject(request: HydraRequest): String = {
    request.metadataValue(IngestionParams.HYDRA_SCHEMA_PARAM).getOrElse(getTopic(request))
  }

}





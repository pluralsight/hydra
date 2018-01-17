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
import hydra.core.ingest.HydraRequest
import hydra.core.transport.ValidationStrategy.Strict
import org.apache.avro.generic.GenericRecord

import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by alexsilva on 1/11/17.
  */
object AvroRecordFactory extends KafkaRecordFactory[String, GenericRecord] with ConfigSupport {

  val schemaRegistry = ConfluentSchemaRegistry.forConfig(applicationConfig)

  lazy val schemaResourceLoader = new SchemaResourceLoader(schemaRegistry.registryUrl,
    schemaRegistry.registryClient)

  override def build(request: HydraRequest)(implicit ec: ExecutionContext): Future[AvroRecord] = {
    for {
      (topic, subject) <- Future.fromTry(getTopicAndSchemaSubject(request))
      res <- schemaResourceLoader.retrieveSchema(subject)
      record <- convert(res, request)
    } yield AvroRecord(topic, res.schema, getKey(request), record)
  }

  private def convert(resource: SchemaResource, request: HydraRequest)
                     (implicit ec: ExecutionContext): Future[GenericRecord] = {
    val converter = new JsonConverter[GenericRecord](resource.schema,
      request.validationStrategy == Strict)
    Future(converter.convert(request.payload))
      .recover { case ex => throw AvroUtils.improveException(ex, resource) }
  }

}



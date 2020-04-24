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

import akka.actor.ActorRef
import akka.pattern.ask
import akka.util
import com.pluralsight.hydra.avro.JsonConverter
import hydra.avro.registry.ConfluentSchemaRegistry
import hydra.avro.resource.SchemaResource
import hydra.avro.util.AvroUtils
import hydra.common.config.ConfigSupport
import hydra.common.logging.LoggingAdapter
import hydra.core.akka.SchemaRegistryActor.{FetchSchemaRequest, FetchSchemaResponse}
import hydra.core.ingest.HydraRequest
import hydra.core.transport.ValidationStrategy.Strict
import org.apache.avro.generic.GenericRecord

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by alexsilva on 1/11/17.
  */
class AvroRecordFactory(schemaResourceLoader: ActorRef)
    extends KafkaRecordFactory[String, GenericRecord]
    with ConfigSupport with LoggingAdapter {

  private implicit val timeout = util.Timeout(3.seconds)

  override def build(
      request: HydraRequest
  )(implicit ec: ExecutionContext): Future[AvroRecord] = {
    for {
      (topic, subject) <- Future.fromTry(getTopicAndSchemaSubject(request))
      schemaResource <- (schemaResourceLoader ? FetchSchemaRequest(subject))
        .mapTo[FetchSchemaResponse]
        .map(_.schemaResource)
      record <- convert(schemaResource, request)
    } yield AvroRecord(
      topic,
      schemaResource.schema,
      getKey(request, record),
      record,
      request.ackStrategy
    )
  }

  private def convert(schemaResource: SchemaResource, request: HydraRequest)(
      implicit ec: ExecutionContext
  ): Future[GenericRecord] = {
    val converter = new JsonConverter[GenericRecord](
      schemaResource.schema,
      request.validationStrategy == Strict
    )
    Future({
      val converted = converter.convert(request.payload)
      converted
    }).recover {
      case ex => throw AvroUtils.improveException(ex, schemaResource,
        ConfluentSchemaRegistry.registryUrl(applicationConfig))
    }
  }
}

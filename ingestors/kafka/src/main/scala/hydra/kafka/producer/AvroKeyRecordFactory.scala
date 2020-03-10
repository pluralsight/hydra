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
import hydra.avro.resource.SchemaResource
import hydra.avro.util.AvroUtils
import hydra.common.config.ConfigSupport
import hydra.core.akka.SchemaRegistryActor.{
  FetchSchemaRequest,
  FetchSchemaResponse
}
import hydra.core.ingest.HydraRequest
import hydra.core.transport.ValidationStrategy
import hydra.core.transport.ValidationStrategy.Strict
import hydra.kafka.producer.AvroKeyRecordFactory.NoKeySchemaFound
import org.apache.avro.generic.GenericRecord

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NoStackTrace

class AvroKeyRecordFactory(schemaResourceLoader: ActorRef)
    extends KafkaRecordFactory[GenericRecord, GenericRecord]
    with ConfigSupport {

  private implicit val timeout = util.Timeout(3.seconds)

  override def build(
      request: HydraRequest
  )(implicit ec: ExecutionContext): Future[AvroKeyRecord] = {
    for {
      (topic, subject) <- Future.fromTry(getTopicAndSchemaSubject(request))
      response <- (schemaResourceLoader ? FetchSchemaRequest(subject))
        .mapTo[FetchSchemaResponse]
      keySchemaResource = response.keySchemaResource.getOrElse(
        throw NoKeySchemaFound
      )
      records <- convertKeyAndValue(
        keySchemaResource,
        response.schemaResource,
        request
      )
    } yield AvroKeyRecord(
      topic,
      keySchemaResource.schema,
      response.schemaResource.schema,
      records._1,
      records._2,
      request.ackStrategy
    )
  }

  private def convertKeyAndValue(
      keySchemaR: SchemaResource,
      valueSchemaR: SchemaResource,
      request: HydraRequest
  )(
      implicit ec: ExecutionContext
  ): Future[(GenericRecord, GenericRecord)] = {
    import spray.json._
    val payload = request.payload.parseJson.asJsObject
    def getFieldAsString(fieldName: String): String =
      payload.fields(fieldName).compactPrint
    for {
      key <- convert(
        keySchemaR,
        request.validationStrategy,
        getFieldAsString("key")
      )
      value <- convert(
        valueSchemaR,
        request.validationStrategy,
        getFieldAsString("value")
      )
    } yield (key -> value)
  }

  private def convert(
      schemaResource: SchemaResource,
      validationStrategy: ValidationStrategy,
      payload: String
  )(
      implicit ec: ExecutionContext
  ): Future[GenericRecord] = {
    val converter = new JsonConverter[GenericRecord](
      schemaResource.schema,
      validationStrategy == Strict
    )
    Future({
      val converted = converter.convert(payload)
      converted
    }).recover {
      case ex => throw AvroUtils.improveException(ex, schemaResource,
        applicationConfig.getString("schema.registry.url"))
    }
  }
}

object AvroKeyRecordFactory {
  case object NoKeySchemaFound extends NoStackTrace
}

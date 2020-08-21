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

package hydra.avro.resource

import java.net.ConnectException
import hydra.avro.registry.SchemaRegistryException
import io.confluent.kafka.schemaregistry.client.{
  SchemaMetadata,
  SchemaRegistryClient
}
import org.apache.avro.Schema
import org.slf4j.LoggerFactory
import scalacache._
import scalacache.guava.GuavaCache
import scalacache.modes.scalaFuture._
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by alexsilva on 1/20/17.
  */

/**
  *
  * @param registryUrl
  * @param registry
  */
class SchemaResourceLoader(
    registryUrl: String,
    registry: SchemaRegistryClient,
    metadataCheckInterval: FiniteDuration = 1 minute
) {

  import SchemaResourceLoader._

  private implicit val guava = SchemaResourceLoader.cache

  def retrieveValueSchema(subject: String, version: Int = 0)(
      implicit ec: ExecutionContext
  ): Future[SchemaResource] = {
    if (version == 0) getLatestSchema(subject.withValueSuffix)
    else loadFromCache(subject.withValueSuffix, version.toString)
  }.map(_.getOrElse(throw SchemaRegistryException(SchemaNotFoundException(subject), subject)))

  def retrieveKeySchema(subject: String, version: Int = 0)(
      implicit ec: ExecutionContext
  ): Future[SchemaResource] = {
    if (version == 0) getLatestSchema(subject.withKeySuffix)
    else loadFromCache(subject.withKeySuffix, version.toString)
  }.map(_.getOrElse(throw SchemaRegistryException(SchemaNotFoundException(subject), subject)))

  def retrieveValueSchemas(subjects: List[String])(
      implicit ec: ExecutionContext
  ): Future[List[(String, Option[SchemaResource])]] = {
    Future.sequence(subjects.map(sub => getLatestSchema(sub.withValueSuffix).map(sch => (sub,sch))))
  }

  def loadValueSchemaIntoCache(
      schemaResource: SchemaResource
  )(implicit ec: ExecutionContext): Future[SchemaResource] = {
    require(schemaResource.id > 0, "A schema id is required.")
    loadSchemaIntoCache(
      schemaResource.schema.getFullName.withValueSuffix,
      schemaResource
    )
  }

  def loadKeySchemaIntoCache(
      schemaResource: SchemaResource
  )(implicit ec: ExecutionContext): Future[SchemaResource] = {
    require(schemaResource.id > 0, "A schema id is required.")
    loadSchemaIntoCache(
      schemaResource.schema.getFullName.withKeySuffix,
      schemaResource
    )
  }

  private def loadSchemaIntoCache(
      srSubject: String,
      schemaResource: SchemaResource
  )(implicit ec: ExecutionContext): Future[SchemaResource] =
    Future
      .sequence {
        Seq(
          schemaCache.put(schemaResource.id)(schemaResource.schema, ttl = None),
          put(srSubject)(Option(schemaResource), ttl = Some(metadataCheckInterval)),
          put(srSubject, schemaResource.version)(Option(schemaResource), ttl = None)
        )
      }
      .map(_ => schemaResource)

  private def getLatestSchema(
      subject: String
  )(implicit ec: ExecutionContext): Future[Option[SchemaResource]] = {
    cachingF(subject)(ttl = Some(metadataCheckInterval)) {
      log.debug(s"Fetching latest metadata for $subject")
      Future(registry.getLatestSchemaMetadata(subject))
        .flatMap { md =>
          schemaCache
            .caching(md.getId)(ttl = None) { //the schema itself is immutable and never expires
              log.debug(
                s"Caching new schema $subject [version=${md.getVersion} id=${md.getId}]"
              )
              new Schema.Parser().parse(md.getSchema)
            }
            .map(sch => Option(SchemaResource(md.getId, md.getVersion, sch)))
        }
        .recover {
          case e: ConnectException => throw e
          case _: Exception        => None
        }
    }
  }

  private def loadFromCache(subject: String, version: String)(
      implicit ec: ExecutionContext
  ): Future[Option[SchemaResource]] = {
    cachingF(subject, version)(ttl = None) {
      log.debug(s"Fetching version $version for $subject schema")
      loadFromRegistry(subject, version)
    }
  }

  private def loadFromRegistry(subject: String, version: String)(
      implicit ec: ExecutionContext
  ): Future[Option[SchemaResource]] = {
    log.debug(
      s"Loading schema $subject, version $version from schema registry $registryUrl."
    )
    Future(version.toInt)
      .map(v => registry.getSchemaMetadata(subject, v))
      .map(toSchemaResource)
      .map(m => {
        registry
          .getById(m.id) //this is what will throw if the schema does not exist
        m
      })
      .map(Option(_))
      .recover {
        case e: ConnectException => throw e
        case _: Exception        => None
      }
  }

  private implicit class AddSuffix(subject: String) {
    private val valueSuffix = "-value"
    private val keySuffix = "-key"

    def withValueSuffix: String = {
      if (subject.endsWith(valueSuffix)) subject else subject + valueSuffix
    }

    def withKeySuffix: String = {
      if (subject.endsWith(keySuffix)) subject else subject + keySuffix
    }
  }

  def toSchemaResource(md: SchemaMetadata): SchemaResource = {
    SchemaResource(
      md.getId,
      md.getVersion,
      new Schema.Parser().parse(md.getSchema)
    )
  }

}

object SchemaResourceLoader {
  val log = LoggerFactory.getLogger(getClass)

  val cache = GuavaCache[Option[SchemaResource]]

  //we need to cache the schemas once and only once, otherwise CachedSchemaRegistryClient used
  // by Confluent inside the KafkaProducer will eventually break once we return more schemas
  // than max.schemas.per.subject.
  val schemaCache = GuavaCache[Schema]

  final case class SchemaNotFoundException(subject: String) extends Exception(s"Schema not found for $subject")
}

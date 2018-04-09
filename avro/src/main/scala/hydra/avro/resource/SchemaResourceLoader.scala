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
import io.confluent.kafka.schemaregistry.client.{SchemaMetadata, SchemaRegistryClient}
import org.apache.avro.Schema
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scalacache._
import scalacache.guava.GuavaCache
import scalacache.modes.scalaFuture._
//import io.confluent.kafka.schemaregistry.client.SchemaResource

/**
  * Created by alexsilva on 1/20/17.
  */

/**
  * We only support two location prefixes: classpath and registry (or no prefix, which defaults
  * to registry.)
  *
  * @param registryUrl
  * @param registry
  * @param suffix
  */
class SchemaResourceLoader(registryUrl: String, registry: SchemaRegistryClient,
                           suffix: String = "-value") {

  import SchemaResourceLoader._

  private implicit val guava = SchemaResourceLoader.cache

  private val defaultCacheTtl = Some(5.minutes)

  def retrieveSchema(subject: String, version: Int)(implicit ec: ExecutionContext): Future[SchemaResource] = {
    loadFromCache(subject.withSuffix, version.toString)
  }

  def retrieveSchema(subject: String)(implicit ec: ExecutionContext) = {
    val parts = subject.split("\\#")
    parts match {
      case Array(subject, version) => loadFromCache(subject.withSuffix, version)
      case Array(subject) => getLatestSchema(subject.withSuffix)
    }
  }

  def loadSchemaIntoCache(schemaResource: SchemaResource)(implicit ec: ExecutionContext): Future[SchemaResource] = {
    val subject = schemaResource.schema.getFullName.withSuffix
    Future.sequence(Seq(put(subject)(schemaResource, ttl = Some(5.minutes)),
      put(subject, schemaResource.version)(schemaResource, ttl = None)))
      .map(_ => schemaResource)
  }

  private def getLatestSchema(subject: String)(implicit ec: ExecutionContext): Future[SchemaResource] = {
    cachingF(subject)(ttl = Some(5.minutes)) {
      log.debug(s"Fetching latest $subject schema")
       Future(registry.getLatestSchemaMetadata(subject)).map(toSchemaResource)
        .recoverWith {
          case e: ConnectException => throw e
          case e: Exception => throw new SchemaRegistryException(e, subject)
        }
    }
  }

  private def loadFromCache(subject: String, version: String)(implicit ec: ExecutionContext): Future[SchemaResource] = {
    cachingF(subject, version)(ttl = None) {
      log.debug(s"Fetching version $version for $subject schema")
      loadFromRegistry(subject, version)
    }
  }

  private def loadFromRegistry(subject: String, version: String)(implicit ec: ExecutionContext): Future[SchemaResource] = {
    log.debug(s"Loading schema $subject, version $version from schema registry $registryUrl.")
    Future(version.toInt).map(v => registry.getSchemaMetadata(subject, v)).map(toSchemaResource)
      .map(m => {
        registry.getByID(m.id) //this is what will throw if the schema does not exist
        m
      })
      .recoverWith {
        case e: ConnectException => throw e
        case e: Exception => throw new SchemaRegistryException(e, subject)
      }
  }

  private implicit class AddSuffix(subject: String) {
    def withSuffix = {
      if (subject.endsWith(suffix)) subject else subject + suffix
    }
  }

  def toSchemaResource(md: SchemaMetadata): SchemaResource = {
    SchemaResource(md.getId, md.getVersion, new Schema.Parser().parse(md.getSchema))
  }

}

object SchemaResourceLoader {
  val log = LoggerFactory.getLogger(getClass)
  val cache = GuavaCache[SchemaResource]
}

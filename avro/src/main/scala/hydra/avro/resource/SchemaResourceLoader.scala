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

import hydra.avro.registry.{ SchemaRegistryException }
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }
import scalacache._
import scalacache.guava.GuavaCache
import scalacache.modes.scalaFuture._
import io.confluent.kafka.schemaregistry.client.SchemaMetadata

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

  private implicit val cache = GuavaCache[SchemaMetadata]
  private val defaultCacheTtl = Some(5.minutes)

  def retrieveSchema(location: String)(implicit ec: ExecutionContext): Future[SchemaMetadata] = {
    require(location ne null)
    val parts = location.split("\\:")
    parts match {
      case Array(prefix, subject) => fetchSchema(subject)
      case Array(subject) => fetchSchema(subject)
      case _ => throw new IllegalArgumentException(s"Unable to parse location $location")
    }
  }

  def retrieveSchema(subject: String, version: Int)(implicit ec: ExecutionContext): Future[SchemaMetadata] = {
    loadFromCache(subject.withSuffix, version.toString)
  }

  def loadSchemaIntoCache(subject: String, metadata: SchemaMetadata)(implicit ec: ExecutionContext) = {
    put(subject.withSuffix)(metadata, ttl = Some(5.minutes))
    put(subject.withSuffix, metadata.getVersion)(metadata, ttl = None)
  }

  private def fetchSchema(subject: String)(implicit ec: ExecutionContext) = {
    val parts = subject.split("\\#")
    parts match {
      case Array(subject, version) => loadFromCache(subject.withSuffix, version)
      case Array(subject) => getLatestSchema(subject.withSuffix)
    }
  }

  private def getLatestSchema(subject: String)(implicit ec: ExecutionContext): Future[SchemaMetadata] = {
    cachingF(subject)(ttl = Some(5.minutes)) {
      Future(registry.getLatestSchemaMetadata(subject))
        .recoverWith {
          case e: ConnectException => throw e
          case e: Exception => throw new SchemaRegistryException(e, subject)
        }
    }
  }

  private def loadFromCache(subject: String, version: String)(implicit ec: ExecutionContext): Future[SchemaMetadata] = {
    cachingF(subject, version)(ttl = None) {
      loadFromRegistry(subject, version)
    }
  }

  private def loadFromRegistry(subject: String, version: String)(implicit ec: ExecutionContext): Future[SchemaMetadata] = {
    log.debug(s"Loading schema $subject, version $version from schema registry $registryUrl.")
    Future(version.toInt).map(v => registry.getSchemaMetadata(subject, v))
      .map(m => {
        registry.getByID(m.getId) //this is what will throw if the schema does not exist
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

}

object SchemaResourceLoader {
  val log = LoggerFactory.getLogger(getClass)
}

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

import hydra.avro.registry.{RegistrySchemaResource, SchemaRegistryException}
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import org.slf4j.LoggerFactory
import org.springframework.core.io.DefaultResourceLoader

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scalacache._
import scalacache.guava.GuavaCache

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

  implicit val cache = ScalaCache(GuavaCache())

  private val loader = new DefaultResourceLoader()

  def retrieveSchema(location: String)(implicit ec: ExecutionContext): Future[SchemaResource] = {
    require(location ne null)

    val parts = location.split("\\:")
    parts match {
      case Array(prefix, location) if prefix == REGISTRY_URL_PREFIX => fetchSchema(location)
      case Array(prefix, location) => Future(GenericSchemaResource(loader.getResource(location)))
      case Array(subject) => fetchSchema(subject)
      case _ => throw new IllegalArgumentException(s"Unable to parse location $location")
    }
  }

  private def fetchSchema(subject: String)(implicit ec: ExecutionContext) = {
    val parts = subject.split("\\#")
    parts match {
      case Array(subject, version) => loadFromCache(subject.withSuffix, version)
      case Array(subject) => getLatestSchema(subject.withSuffix)
    }
  }

  private def getLatestSchema(subject: String)
                             (implicit ec: ExecutionContext): Future[RegistrySchemaResource] = {
    cachingWithTTL(subject)(5.minutes) {
      Future(registry.getLatestSchemaMetadata(subject))
        .map(md => registry.getByID(md.getId) -> md)
        .map(schema => RegistrySchemaResource(registryUrl, subject, schema._2.getId, schema._2.getVersion, schema._1))
        .recoverWith {
          case e: ConnectException => Future.failed(e)
          case e: Exception => Future.failed(SchemaRegistryException(e, subject))
        }
    }
  }

  private def loadFromCache(subject: String, version: String)
                           (implicit ec: ExecutionContext): Future[RegistrySchemaResource] = {
    cachingWithTTL(subject, version)(5.minutes) {
      loadFromRegistry(subject, version)
    }
  }

  private def loadFromRegistry(subject: String, version: String)
                              (implicit ec: ExecutionContext): Future[RegistrySchemaResource] = {
    log.debug(s"Loading schema $subject, version $version from schema registry $registryUrl.")
    Future(version.toInt).map(v => registry.getSchemaMetadata(subject, v))
      .map(m => registry.getByID(m.getId) -> m)
      .map(schema => RegistrySchemaResource(registryUrl, subject, schema._2.getId(), version.toInt, schema._1))
      .recoverWith {
        case e: Exception => Future.failed(SchemaRegistryException(e, subject))
      }
  }

  private implicit class AddSuffix(location: String) {
    def withSuffix = if (location.endsWith(suffix)) location else location + suffix
  }

}

object SchemaResourceLoader {

  val REGISTRY_URL_PREFIX = "registry"

  val log = LoggerFactory.getLogger(getClass)

}


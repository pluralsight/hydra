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

package hydra.core.avro.schema

import hydra.common.logging.LoggingAdapter
import hydra.core.avro.registry.{RegistrySchemaResource, SchemaRegistryException}
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import org.springframework.core.io.DefaultResourceLoader

import scala.concurrent.duration._
import scala.util.{Failure, Try}
import scalacache._
import scalacache.guava.GuavaCache

/**
  * Created by alexsilva on 1/20/17.
  */
class SchemaResourceLoader(registryUrl: String, registry: SchemaRegistryClient, suffix: String = "-value")
  extends DefaultResourceLoader with LoggingAdapter {

  private implicit val cache = ScalaCache(GuavaCache())


  override def getResource(location: String): SchemaResource = {
    require(location ne null)
    val parts = location.split("\\:")
    parts match {
      case Array(prefix, location) if prefix == SchemaResourceLoader.REGISTRY_URL_PREFIX => retrieveSchema(location).get
      case Array(prefix, location) => GenericSchemaResource(super.getResource(location))
      case Array(subject) => retrieveSchema(subject).get
      case _ => throw new IllegalArgumentException(s"Unable to parse location $location")
    }
  }

  private def getLatestSchema(subject: String): Try[RegistrySchemaResource] = {
    Try(registry.getLatestSchemaMetadata(subject))
      .map(md => registry.getByID(md.getId) -> md)
      .map(schema => RegistrySchemaResource(registryUrl, subject, schema._2.getId, schema._2.getVersion, schema._1))
      .recoverWith {
        case e: Exception => Failure(SchemaRegistryException(e, subject))
      }
  }

  private def loadFromCache(subject: String, version: String): Try[RegistrySchemaResource] = {
    sync.cachingWithTTL(s"$subject.$version")(5.minutes) {
      loadFromRegistry(subject, version)
    }
  }

  private def retrieveSchema(subject: String) = {
    val parts = subject.split("\\#")
    parts match {
      case Array(subject, version) => loadFromCache(subject.withSuffix, version)
      case Array(subject) => getLatestSchema(subject.withSuffix) //can't cache this!
    }
  }

  private def loadFromRegistry(subject: String, version: String): Try[RegistrySchemaResource] = {
    log.debug(s"Loading schema $subject, version $version from schema registry $registryUrl.")
    Try(version.toInt).map(v => registry.getSchemaMetadata(subject, v))
      .map(m => registry.getByID(m.getId) -> m)
      .map(schema => RegistrySchemaResource(registryUrl, subject, schema._2.getId(), version.toInt, schema._1))
      .recoverWith {
        case e: Exception => Failure(SchemaRegistryException(e, subject))
      }
  }

  private implicit class AddSuffix(location: String) {
    def withSuffix = if (location.endsWith(suffix)) location else location + suffix
  }

}

object SchemaResourceLoader {
  val REGISTRY_URL_PREFIX = "registry"

}


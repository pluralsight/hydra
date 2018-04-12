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

import hydra.avro.registry.SchemaRegistryException
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import org.apache.avro.{Schema, SchemaBuilder}
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{BeforeAndAfterEach, FunSpecLike, Matchers}

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.io.Source
import scalacache.modes.sync._

/**
  * Created by alexsilva on 1/20/17.
  */
class SchemaResourceLoaderSpec extends Matchers
  with FunSpecLike
  with BeforeAndAfterEach
  with Eventually
  with ScalaFutures {

  implicit override val patienceConfig =
    PatienceConfig(timeout = scaled(Span(1, Seconds)), interval = scaled(Span(10, Millis)))

  val schemaParser = new Schema.Parser()
  val testSchema = schemaParser.parse(Source.fromResource("resource-loader-spec.avsc").mkString)
  val subject = testSchema.getFullName

  override def beforeEach = SchemaResourceLoader.schemaCache.removeAll()

  def fixture() = {
    val client = new MockSchemaRegistryClient
    client.register(s"${testSchema.getFullName}-value", testSchema)

    new SchemaResourceLoader("http://mock", client)
  }

  describe("When loading schemas from the registry") {
    it("returns the latest version of the schema") {
      val loader = fixture()
      val res = loader.retrieveSchema(subject)
      whenReady(res) { schemaMetadata =>
        schemaMetadata.schema shouldBe testSchema
      }
    }

    it("loads a schema with an explicit version") {
      val loader = fixture()
      val res = loader.retrieveSchema(s"${subject}#1")
      whenReady(res) { schemaMetadata =>
        schemaMetadata.schema shouldBe testSchema
      }
    }

    it("errors if can't find a schema with a specific version") {
      val loader = fixture()
      val res = loader.retrieveSchema(s"${subject}#2").failed
      whenReady(res) { error =>
        error shouldBe a[SchemaRegistryException]
        error.getMessage should not be null
      }
    }

    it("errors when subject is not known") {
      val loader = fixture()
      whenReady(loader.retrieveSchema("registry:tester").failed)(_ shouldBe a[SchemaRegistryException])
    }

    it("loads a previously cached schema from the cache") {
      //create a client that if called will blow up
      val client = new MockSchemaRegistryClient
      val loader = new SchemaResourceLoader("http://localhost:48223", client)
      val expectedSchemaResource = SchemaResource(1, 1, testSchema)
      whenReady(loader.loadSchemaIntoCache(expectedSchemaResource)) { _ =>
        val res = loader.retrieveSchema("hydra.test.Tester")
        whenReady(res) { schemaMetadata =>
          schemaMetadata.schema shouldBe testSchema
        }
      }
    }

    it("can get a schema by subject and version") {
      val client = new MockSchemaRegistryClient
      val loader = new SchemaResourceLoader("http://localhost:48223", client)
      val expectedSchemaResource = SchemaResource(1, 1, testSchema)
      loader.loadSchemaIntoCache(expectedSchemaResource)
      val res = loader.retrieveSchema(testSchema.getFullName, 1)
      whenReady(res) { schemaResource =>
        schemaResource.schema shouldBe testSchema
      }
    }

    it("returns the same underlying schema instance if the registry metadata hasn't changed") {
      val client = new MockSchemaRegistryClient
      client.register(testSchema.getFullName + "-value", testSchema)
      val loader = new SchemaResourceLoader("http://localhost:48223", client,
        metadataCheckInterval = 5.millis)
      whenReady(loader.retrieveSchema(testSchema.getFullName)) { schemaResource =>
        schemaResource.schema shouldBe testSchema
      }
      eventually {
        whenReady(loader.retrieveSchema(testSchema.getFullName)) { schemaResource =>
          (schemaResource.schema eq testSchema) shouldBe true
        }
      }
    }

    it("updates the underlying schema instance when the registry metadata changes") {
      val nschema = newSchema(testSchema.getNamespace, "ntest")
      val client = new MockSchemaRegistryClient
      client.register(nschema.getFullName + "-value", nschema)
      val loader = new SchemaResourceLoader("http://localhost:48223", client,
        metadataCheckInterval = 5.millis)
      whenReady(loader.retrieveSchema(nschema.getFullName)) { schemaResource =>
        schemaResource.schema shouldBe nschema
      }

      //evolve the schema
      val field = new Schema.Field("new_field", SchemaBuilder.builder().intType(), "NewField", 10)
      val fields = nschema.getFields.asScala.map { f =>
        new Schema.Field(f.name(), f.schema(), f.doc(), f.defaultVal())
      } :+ field
      val evolvedSchema = Schema.createRecord(nschema.getName(),
        "evolve", nschema.getNamespace(), false, fields.asJava)
      client.register(nschema.getFullName + "-value", evolvedSchema)
      eventually {
        whenReady(loader.retrieveSchema(nschema.getFullName)) { schemaResource =>
          (schemaResource.schema eq nschema) shouldBe false
        }
      }
    }
  }

  private def newSchema(namespace: String, name: String): Schema = {
    val fields = testSchema.getFields.asScala.map { f =>
      new Schema.Field(f.name(), f.schema(), f.doc(), f.defaultVal())
    }
    Schema.createRecord(name, "evolve", namespace, false, fields.asJava)
  }
}

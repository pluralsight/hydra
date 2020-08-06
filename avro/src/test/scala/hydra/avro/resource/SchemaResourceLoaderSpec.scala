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
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.funspec.AnyFunSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import scalacache.modes.sync._

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

/**
  * Created by alexsilva on 1/20/17.
  */
class SchemaResourceLoaderSpec
    extends Matchers
    with AnyFunSpecLike
    with BeforeAndAfterEach
    with Eventually
    with ScalaFutures {

  implicit override val patienceConfig =
    PatienceConfig(
      timeout = scaled(Span(1, Seconds)),
      interval = scaled(Span(10, Millis))
    )

  val schemaParser = new Schema.Parser()

  val testValueSchema = schemaParser.parse(
    Thread
      .currentThread()
      .getContextClassLoader
      .getResourceAsStream("resource-loader-spec.avsc")
  )

  val testKeySchema = schemaParser.parse(
    Thread
      .currentThread()
      .getContextClassLoader
      .getResourceAsStream("resource-loader-spec-key.avsc")
  )

  val subject = testValueSchema.getFullName

  override def beforeEach = SchemaResourceLoader.schemaCache.removeAll()

  def fixture() = {
    val client = new MockSchemaRegistryClient
    client.register(s"${testValueSchema.getFullName}-value", testValueSchema)
    client.register(s"${testValueSchema.getFullName}-key", testValueSchema)

    new SchemaResourceLoader("http://mock", client)
  }

  describe("When loading schemas from the registry") {

    it("return a list of schemas") {
      val loader = fixture()
      val res = loader.retrieveValueSchemas(List(subject, subject))
      whenReady(res) { metadatas =>
        metadatas.map(_ match {
          case (subj, Some(schemaResource)) => (subj, schemaResource.schema)
          case _ => None
        }) shouldBe List((subject,testValueSchema), (subject, testValueSchema))
      }
    }

    it("returns the latest version of a value schema") {
      val loader = fixture()
      val res = loader.retrieveValueSchema(subject)
      whenReady(res) { schemaMetadata =>
        schemaMetadata.schema shouldBe testValueSchema
      }
    }

    it("returns the latest version of a key schema") {
      val loader = fixture()
      val res = loader.retrieveKeySchema(subject)
      whenReady(res) { schemaMetadata =>
        schemaMetadata.schema shouldBe testValueSchema
      }
    }

    it("errors if can't find a value schema with a specific version") {
      val loader = fixture()
      val res = loader.retrieveValueSchema(s"${subject}", 20).failed
      whenReady(res) { error =>
        error shouldBe a[SchemaRegistryException]
        error.getMessage should not be null
      }
    }

    it("errors if can't find a key schema with a specific version") {
      val loader = fixture()
      val res = loader.retrieveKeySchema(s"${subject}", 20).failed
      whenReady(res) { error =>
        error shouldBe a[SchemaRegistryException]
        error.getMessage should not be null
      }
    }

    it("errors when a value schema subject is not known") {
      val loader = fixture()
      whenReady(loader.retrieveValueSchema("registry:tester").failed)(
        _ shouldBe a[SchemaRegistryException]
      )
    }

    it("errors when a key schema subject is not known") {
      val loader = fixture()
      whenReady(loader.retrieveKeySchema("registry:tester").failed)(
        _ shouldBe a[SchemaRegistryException]
      )
    }

    it("loads a previously cached value schema from the cache") {
      //create a client that if called will blow up
      val client = new MockSchemaRegistryClient
      val loader = new SchemaResourceLoader("http://localhost:48223", client)
      val expectedSchemaResource = SchemaResource(1, 1, testValueSchema)
      whenReady(loader.loadValueSchemaIntoCache(expectedSchemaResource)) { _ =>
        val res = loader.retrieveValueSchema("hydra.test.Tester")
        whenReady(res) { schemaMetadata =>
          schemaMetadata.schema shouldBe testValueSchema
        }
      }
    }

    it("loads a previously cached key schema from the cache") {
      //create a client that if called will blow up
      val client = new MockSchemaRegistryClient
      val loader = new SchemaResourceLoader("http://localhost:48223", client)
      val expectedSchemaResource = SchemaResource(1, 1, testKeySchema)
      whenReady(loader.loadKeySchemaIntoCache(expectedSchemaResource)) { _ =>
        val res = loader.retrieveKeySchema("hydra.test.KeyTester")
        whenReady(res) { schemaMetadata =>
          schemaMetadata.schema shouldBe testKeySchema
        }
      }
    }

    it("gets a value schema by subject and version") {
      val client = new MockSchemaRegistryClient
      val loader = new SchemaResourceLoader("http://localhost:48223", client)
      val expectedSchemaResource = SchemaResource(1, 1, testValueSchema)
      loader.loadValueSchemaIntoCache(expectedSchemaResource)
      val res = loader.retrieveValueSchema(testValueSchema.getFullName, 1)
      whenReady(res) { schemaResource =>
        schemaResource.schema shouldBe testValueSchema
      }
    }

    it("gets a key schema by subject and version") {
      val client = new MockSchemaRegistryClient
      val loader = new SchemaResourceLoader("http://localhost:48223", client)
      val expectedSchemaResource = SchemaResource(1, 1, testKeySchema)
      loader.loadKeySchemaIntoCache(expectedSchemaResource)
      val res = loader.retrieveKeySchema(testKeySchema.getFullName, 1)
      whenReady(res) { schemaResource =>
        schemaResource.schema shouldBe testKeySchema
      }
    }

    it(
      "returns the same underlying value schema instance if the registry metadata hasn't changed"
    ) {
      val client = new MockSchemaRegistryClient
      client.register(testValueSchema.getFullName + "-value", testValueSchema)
      val loader = new SchemaResourceLoader(
        "http://localhost:48223",
        client,
        metadataCheckInterval = 5.millis
      )
      whenReady(loader.retrieveValueSchema(testValueSchema.getFullName)) {
        schemaResource => schemaResource.schema shouldBe testValueSchema
      }

      eventually {
        whenReady(loader.retrieveValueSchema(testValueSchema.getFullName)) {
          schemaResource =>
            (schemaResource.schema eq testValueSchema) shouldBe true
        }
      }
    }

    it(
      "returns the same underlying key schema instance if the registry metadata hasn't changed"
    ) {
      val client = new MockSchemaRegistryClient
      client.register(testKeySchema.getFullName + "-key", testKeySchema)
      val loader = new SchemaResourceLoader(
        "http://localhost:48223",
        client,
        metadataCheckInterval = 5.millis
      )
      whenReady(loader.retrieveKeySchema(testKeySchema.getFullName)) {
        schemaResource => schemaResource.schema shouldBe testKeySchema
      }

      eventually {
        whenReady(loader.retrieveKeySchema(testKeySchema.getFullName)) {
          schemaResource =>
            (schemaResource.schema eq testKeySchema) shouldBe true
        }
      }
    }

    it(
      "updates the underlying schema instance when the registry metadata changes"
    ) {
      val nschema = newValueSchema(testValueSchema.getNamespace, "ntest")
      val client = new MockSchemaRegistryClient
      client.register(nschema.getFullName + "-value", nschema)
      val loader = new SchemaResourceLoader(
        "http://localhost:48223",
        client,
        metadataCheckInterval = 5.millis
      )
      whenReady(loader.retrieveValueSchema(nschema.getFullName)) {
        schemaResource => schemaResource.schema shouldBe nschema
      }

      //evolve the schema
      val field = new Schema.Field(
        "new_field",
        SchemaBuilder.builder().intType(),
        "NewField",
        10
      )
      val fields = nschema.getFields.asScala.map { f =>
        new Schema.Field(f.name(), f.schema(), f.doc(), f.defaultVal())
      } :+ field
      val evolvedSchema = Schema.createRecord(
        nschema.getName(),
        "evolve",
        nschema.getNamespace(),
        false,
        fields.asJava
      )
      client.register(nschema.getFullName + "-value", evolvedSchema)
      eventually {
        whenReady(loader.retrieveValueSchema(nschema.getFullName)) {
          schemaResource => (schemaResource.schema eq nschema) shouldBe false
        }
      }
    }

    it(
      "updates the underlying key schema instance when the registry metadata changes"
    ) {
      val nschema = newKeySchema(testKeySchema.getNamespace, "ntest")
      val client = new MockSchemaRegistryClient
      client.register(nschema.getFullName + "-value", nschema)
      val loader = new SchemaResourceLoader(
        "http://localhost:48223",
        client,
        metadataCheckInterval = 5.millis
      )
      eventually {
        whenReady(loader.retrieveValueSchema(nschema.getFullName)) {
          schemaResource => schemaResource.schema shouldBe nschema
        }
      }

      //evolve the schema
      val field = new Schema.Field(
        "new_field",
        SchemaBuilder.builder().intType(),
        "NewField",
        10
      )
      val fields = nschema.getFields.asScala.map { f =>
        new Schema.Field(f.name(), f.schema(), f.doc(), f.defaultVal())
      } :+ field
      val evolvedSchema = Schema.createRecord(
        nschema.getName(),
        "evolve",
        nschema.getNamespace(),
        false,
        fields.asJava
      )
      client.register(nschema.getFullName + "-value", evolvedSchema)
      eventually {
        whenReady(loader.retrieveValueSchema(nschema.getFullName)) {
          schemaResource => (schemaResource.schema eq nschema) shouldBe false
        }
      }
    }
  }

  private def newValueSchema(namespace: String, name: String): Schema = {
    val fields = testValueSchema.getFields.asScala.map { f =>
      new Schema.Field(f.name(), f.schema(), f.doc(), f.defaultVal())
    }
    Schema.createRecord(name, "evolve", namespace, false, fields.asJava)
  }

  private def newKeySchema(namespace: String, name: String): Schema = {
    val fields = testKeySchema.getFields.asScala.map { f =>
      new Schema.Field(f.name(), f.schema(), f.doc(), f.defaultVal())
    }
    Schema.createRecord(name, "evolve", namespace, false, fields.asJava)
  }
}

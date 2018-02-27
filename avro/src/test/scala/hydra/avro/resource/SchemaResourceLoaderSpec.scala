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

import java.io.File
import java.net.URL

import hydra.avro.registry.SchemaRegistryException
import io.confluent.kafka.schemaregistry.client.{ MockSchemaRegistryClient, SchemaMetadata }
import org.apache.avro.Schema.Parser
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{ Millis, Seconds, Span }
import org.scalatest.{ BeforeAndAfterAll, FunSpecLike, Matchers }
import org.apache.avro.Schema
import scala.concurrent.ExecutionContext.Implicits.global
import scala.io.Source

/**
 * Created by alexsilva on 1/20/17.
 */
class SchemaResourceLoaderSpec extends Matchers
  with FunSpecLike
  with BeforeAndAfterAll
  with ScalaFutures {

  implicit override val patienceConfig =
    PatienceConfig(timeout = scaled(Span(1, Seconds)), interval = scaled(Span(10, Millis)))

  val schemaParser = new Schema.Parser()
  val testSchema = schemaParser.parse(Source.fromResource("resource-loader-spec.avsc").mkString)

  def fixture() = {
    val client = new MockSchemaRegistryClient
    client.register("test-value", testSchema)
    new SchemaResourceLoader("http://mock", client)
  }

  describe("When loading schemas from the registry") {
    it("returns the latest version of the schema") {
      val loader = fixture()
      val res = loader.retrieveSchema("registry:test-value")
      whenReady(res) { schemaMetadata =>
        new Schema.Parser().parse(schemaMetadata.getSchema) shouldBe testSchema
      }
    }

    it("loads a schema with an explicit version") {
      val loader = fixture()
      val res = loader.retrieveSchema("registry:test-value#1")
      whenReady(res) { schemaMetadata =>
        new Schema.Parser().parse(schemaMetadata.getSchema) shouldBe testSchema
      }
    }

    it("errors if can't find a schema with a specific version") {
      val loader = fixture()
      val res = loader.retrieveSchema("registry:test-value#2").failed
      whenReady(res) { error =>
        error shouldBe a[SchemaRegistryException]
        error.getMessage should not be null
      }
    }

    it("defaults to registry resources") {
      val loader = fixture()
      val res = loader.retrieveSchema("test-value#1")
      whenReady(res) { schemaMetadata =>
        new Schema.Parser().parse(schemaMetadata.getSchema) shouldBe testSchema
      }
    }

    it("errors when subject is not known") {
      val loader = fixture()
      whenReady(loader.retrieveSchema("registry:tester").failed)(_ shouldBe a[SchemaRegistryException])
    }

    it("adds the suffix") {
      val loader = fixture()
      val res = loader.retrieveSchema("test#1")
      whenReady(res) { schema =>
        new Schema.Parser().parse(schema.getSchema) shouldBe testSchema
      }
    }

    it("can add a schema to the cache") {
      //create a client that if called will blow up
      val client = new MockSchemaRegistryClient
      val loader = new SchemaResourceLoader("http://localhost:48223", client)
      val expectedSchemaMetadata = new SchemaMetadata(1, 1, testSchema.toString)
      loader.loadSchemaIntoCache("test", expectedSchemaMetadata)
      val res = loader.retrieveSchema("test")
      whenReady(res) { schemaMetadata =>
        new Schema.Parser().parse(schemaMetadata.getSchema) shouldBe testSchema
      }
    }

    it("can get a schema by subject and version") {
      val client = new MockSchemaRegistryClient
      val loader = new SchemaResourceLoader("http://localhost:48223", client)
      val expectedSchemaMetadata = new SchemaMetadata(1, 1, testSchema.toString)
      loader.loadSchemaIntoCache("test", expectedSchemaMetadata)
      val res = loader.retrieveSchema("test", 1)
      whenReady(res) { schemaMetadata =>
        new Schema.Parser().parse(schemaMetadata.getSchema) shouldBe testSchema
      }
    }
  }
}

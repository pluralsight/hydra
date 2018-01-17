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
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import org.apache.avro.Schema.Parser
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}
import scala.concurrent.ExecutionContext.Implicits.global

import scala.io.Source

/**
  * Created by alexsilva on 1/20/17.
  */
class SchemaResourceLoaderSpec extends Matchers
  with FunSpecLike
  with BeforeAndAfterAll
  with ScalaFutures {

  val client = new MockSchemaRegistryClient
  val testSchema = Thread.currentThread().getContextClassLoader.getResource("schema.avsc").getFile

  override def beforeAll() = {
    client.register("test-value", new Parser().parse(new File(testSchema)))
  }

  describe("When loading schemas from the registry") {
    it("creates a resource based on latest version") {
      val loader = new SchemaResourceLoader("http://mock", client)
      val res = loader.retrieveSchema("registry:test-value")
      whenReady(res) { schema =>
        schema.exists() shouldBe true
        schema.getURL shouldBe new URL("http://mock/ids/1")
        schema.isReadable shouldBe true
        new Parser().parse(Source.fromInputStream(schema.getInputStream).mkString) shouldBe
          new Parser().parse(Source.fromFile(testSchema).mkString)
      }
    }

    it("loads a schema with an explicit version") {
      val loader = new SchemaResourceLoader("http://mock", client)
      val res = loader.retrieveSchema("registry:test-value#1")
      whenReady(res) { schema =>
        schema.exists() shouldBe true
        schema.getURL shouldBe new URL("http://mock/ids/1")
        schema.isReadable shouldBe true
        new Parser().parse(Source.fromInputStream(schema.getInputStream).mkString) shouldBe
          new Parser().parse(Source.fromFile(testSchema).mkString)
      }
    }

    it("errors if can't find a schema with a specific version") {
      val loader = new SchemaResourceLoader("http://mock", client)
      whenReady(loader.retrieveSchema("registry:test-value#2")
        .failed)(_ shouldBe a[SchemaRegistryException])
    }

    it("defaults to registry resources") {
      val loader = new SchemaResourceLoader("http://mock", client)
      whenReady(loader.retrieveSchema("test-value#1")) { res =>
        res.exists() shouldBe true
        res.getURL shouldBe new URL("http://mock/ids/1")
        res.isReadable shouldBe true
        new Parser().parse(Source.fromInputStream(res.getInputStream).mkString) shouldBe
          new Parser().parse(Source.fromFile(testSchema).mkString)
      }
    }

    it("understands classpath resources") {
      val loader = new SchemaResourceLoader("http://mock", client)
      whenReady(loader.retrieveSchema("classpath:schema.avsc")) { res =>
        res.exists() shouldBe true
        res.isReadable shouldBe true
        new Parser().parse(Source.fromInputStream(res.getInputStream).mkString) shouldBe
          new Parser().parse(Source.fromFile(testSchema).mkString)
      }
    }


    it("errors when subject is not known") {
      val loader = new SchemaResourceLoader("http://mock", client)
      whenReady(loader.retrieveSchema("registry:tester")
        .failed)(_ shouldBe a[SchemaRegistryException])
    }

    it("adds the suffix") {
      val loader = new SchemaResourceLoader("http://mock", client)
      whenReady(loader.retrieveSchema("test#1")) { res =>
        res.exists() shouldBe true
        res.getURL shouldBe new URL("http://mock/ids/1")
        res.isReadable shouldBe true
        new Parser().parse(Source.fromInputStream(res.getInputStream).mkString) shouldBe
          new Parser().parse(Source.fromFile(testSchema).mkString)
      }
    }
  }

}

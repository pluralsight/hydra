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

package hydra.core.avro.io

import java.io.{File, IOException}
import java.net.URL

import hydra.core.avro.registry.SchemaRegistryException
import hydra.core.avro.schema.SchemaResourceLoader
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import org.apache.avro.Schema.Parser
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

import scala.io.Source

/**
  * Created by alexsilva on 1/20/17.
  */
class SchemaResourceLoaderSpec extends Matchers with FunSpecLike with BeforeAndAfterAll {
  val client = new MockSchemaRegistryClient
  val schema = Thread.currentThread().getContextClassLoader.getResource("schema.avsc").getFile

  override def beforeAll() = {
    client.register("test-value", new Parser().parse(new File(schema)))
  }

  describe("When loading schemas from the registry") {
    it("creates a resource based on latest version") {
      val loader = new SchemaResourceLoader("http://mock", client)
      val res = loader.getResource("registry:test-value")
      res.exists() shouldBe true
      res.getURL shouldBe new URL("http://mock/ids/1")
      res.isReadable shouldBe true
      new Parser().parse(Source.fromInputStream(res.getInputStream).mkString) shouldBe
        new Parser().parse(Source.fromFile(schema).mkString)
    }

    it("loads a schema with an explicit version") {
      val loader = new SchemaResourceLoader("http://mock", client)
      val res = loader.getResource("registry:test-value#1")
      res.exists() shouldBe true
      res.getURL shouldBe new URL("http://mock/ids/1")
      res.isReadable shouldBe true
      new Parser().parse(Source.fromInputStream(res.getInputStream).mkString) shouldBe
        new Parser().parse(Source.fromFile(schema).mkString)
    }

    it("errors if can't find a schema with a specific version") {
      val loader = new SchemaResourceLoader("http://mock", client)
      intercept[SchemaRegistryException]{
        loader.getResource("registry:test-value#2")
      }
    }

    it("defaults to registry resources") {
      val loader = new SchemaResourceLoader("http://mock", client)
      val res = loader.getResource("test-value#1")
      res.exists() shouldBe true
      res.getURL shouldBe new URL("http://mock/ids/1")
      res.isReadable shouldBe true
      new Parser().parse(Source.fromInputStream(res.getInputStream).mkString) shouldBe
        new Parser().parse(Source.fromFile(schema).mkString)
    }

    it("understands classpath resources") {
      val loader = new SchemaResourceLoader("http://mock", client)
      val res = loader.getResource("classpath:schema.avsc")
      res.exists() shouldBe true
      res.isReadable shouldBe true
      new Parser().parse(Source.fromInputStream(res.getInputStream).mkString) shouldBe
        new Parser().parse(Source.fromFile(schema).mkString)
    }


    it("errors when subject is not known") {
      val loader = new SchemaResourceLoader("http://mock", client)
      intercept[SchemaRegistryException] {
        loader.getResource("registry:tester")
      }
    }

    it("adds the suffix") {
      val loader = new SchemaResourceLoader("http://mock", client)
      val res = loader.getResource("test#1")
      res.exists() shouldBe true
      res.getURL shouldBe new URL("http://mock/ids/1")
      res.isReadable shouldBe true
      new Parser().parse(Source.fromInputStream(res.getInputStream).mkString) shouldBe
        new Parser().parse(Source.fromFile(schema).mkString)
    }
  }

}

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

package hydra.core.avro.util

import com.typesafe.config.ConfigFactory
import hydra.core.avro.registry.ConfluentSchemaRegistry
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, MockSchemaRegistryClient}
import org.scalatest.{FunSpecLike, Matchers}

/**
  * Created by alexsilva on 9/16/16.
  */
class ConfluentSchemaRegistrySpec extends Matchers with FunSpecLike with ConfluentSchemaRegistry {

  describe("When creating a schema registry client") {
    it("returns a mock") {
      val config = ConfigFactory.parseString("schema.registry.url=mock")
      registry shouldBe a[MockSchemaRegistryClient]
      registryUrl shouldBe "mock"
    }

    it("throws an error if no config key is found") {
      val config = ConfigFactory.empty
      intercept[IllegalArgumentException] {
        ConfluentSchemaRegistry.fromConfig(config)
      }
    }

    it("returns a cached client when using a url") {
      val config = ConfigFactory.parseString("schema.registry.url=\"http://localhost:9092\"")
      ConfluentSchemaRegistry.fromConfig(config) shouldBe a[CachedSchemaRegistryClient]
      ConfluentSchemaRegistry.registryUrl(config) shouldBe "http://localhost:9092"
    }
  }

}

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

package hydra.avro.util

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import org.scalatest.matchers.should.Matchers
import org.scalatest.funspec.AnyFunSpecLike

import scala.io.Source

/**
  * Created by alexsilva on 10/11/16.
  */
class AvroSchemaGeneratorSpec extends Matchers with AnyFunSpecLike {

  val mapper = new ObjectMapper
  val converter = new AvroSchemaGenerator()

  val json = Source
    .fromFile(
      Thread.currentThread.getContextClassLoader
        .getResource("avro-test.json")
        .getFile
    )
    .getLines()
    .mkString

  describe("The json to avro schema converter") {
    it("Should include a valid namespace and a valid name") {
      val jsonNode = mapper.readTree(converter.convert(json, "hydra", "name"))
      jsonNode.at("/namespace").asText shouldBe "hydra"
      jsonNode.at("/name").asText shouldBe "name"
      jsonNode.at("/type").asText shouldBe "record"
    }
    it("Should have a valid record type") {
      val jsonNode = mapper.readTree(converter.convert(json, "hydra", "name"))
      val arrayNode = jsonNode.at("/fields")
      arrayNode.get(0).at("/type/type").asText shouldBe "record"
    }

    it("Should throw an exception with null values") {
      val jsonNode = mapper.readTree(json)
      jsonNode.asInstanceOf[ObjectNode].set("dummyString", null)
      intercept[IllegalArgumentException] {
        converter.convert(jsonNode.toString(), "hydra", "name")
      }
    }
    it("Should convert booleans") {
      val clipViewJson =
        """
          |{
          |	"clipId": "shawn-wildermuth|front-end-web-app-html5-javascript-css-m01|front-end-web-app-html5-javascript-css-m1-02",
          |	"clipModuleIndex": 1,
          |	"clipName": "front-end-web-app-html5-javascript-css-m1-02",
          |	"contentIndexPosition": 99999,
          |	"countsTowardTrialLimits": false,
          |	"courseName": "front-end-web-app-html5-javascript-css",
          |	"courseTitle": "Front-End Web Development Quick Start With HTML5, CSS, and JavaScript",
          |	"ipAddress": "127.0.0.1",
          |	"moduleAuthorHandle": "shawn-wildermuth",
          |	"moduleId": "shawn-wildermuth|front-end-web-app-html5-javascript-css-m01",
          |	"moduleName": "front-end-web-app-html5-javascript-css-m01",
          |	"online": true,
          |	"royaltiesPaid": true,
          |	"started": "2016-11-30T20:30:45.3136582Z",
          |	"userHandle": "44bbf444-ba44-444a-b444-b444bebb4b4b"
          |}
        """.stripMargin

      val jsonNode = mapper.readTree(clipViewJson)

      val schema = converter.convert(jsonNode.toString(), "hydra", "name")

      println(schema)
    }
  }
}

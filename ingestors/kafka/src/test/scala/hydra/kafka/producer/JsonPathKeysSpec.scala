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

package hydra.kafka.producer

import org.scalatest.matchers.should.Matchers
import org.scalatest.funspec.AnyFunSpecLike

/**
  * Created by alexsilva on 1/11/17.
  */
class JsonPathKeysSpec extends Matchers with AnyFunSpecLike {
  import spray.json._

  describe("When using json path") {
    it("parses static keys") {
      JsonPathKeys.getKey("test", "test") shouldBe "test"
      JsonPathKeys.getKey(
        "123",
        """{"id":"1","host":"host","port":9092}""".parseJson
      ) shouldBe "123"
    }

    it("parses keys from strings") {
      JsonPathKeys.getKey("{$.id}", """{"id":"1","host":"host","port":9092}""") shouldBe "1"
    }

    it("parses keys from JsValue objects") {
      JsonPathKeys.getKey(
        "{$.id}",
        """{"id":"1","host":"host","port":9092}""".parseJson
      ) shouldBe "1"
    }
  }
}

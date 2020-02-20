package hydra.kafka.serializers


import com.fasterxml.jackson.databind.ObjectMapper

import scala.collection.JavaConverters._
import org.scalatest.{FunSpecLike, Matchers}

class JsonSerializerSpec extends Matchers with FunSpecLike {

  val mapper = new ObjectMapper()

  describe("The JSON serializer") {
    it("serializes values") {
      val jsonSerializer = new JsonSerializer()
      jsonSerializer.configure(Map.empty[String, Any].asJava, false)
      val node = mapper.readTree("""{"name":"hydra"}""")
      jsonSerializer.serialize("topic", node) shouldBe mapper.writeValueAsBytes(node)
      jsonSerializer.close()
    }
  }

  describe("The JSON deserializer") {
    it("de-serializes values") {
      val jd = new JsonDeserializer()
      jd.configure(Map.empty[String, Any].asJava, false)
      val node = mapper.readTree("""{"name":"hydra"}""")
      jd.deserialize("topic", mapper.writeValueAsBytes(node)) shouldBe node
      jd.close()
    }
  }
}

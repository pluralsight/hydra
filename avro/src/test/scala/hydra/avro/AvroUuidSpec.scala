package hydra.avro

import hydra.avro.convert.AvroUuid
import org.apache.avro.Schema
import org.scalatest.{FlatSpecLike, Matchers}

class AvroUuidSpec extends Matchers with FlatSpecLike {

  "The AvroUuid class" should "validate correct types" in {
    AvroUuid.validate(Schema.create(Schema.Type.STRING))
    intercept[IllegalArgumentException] {
      AvroUuid.validate(Schema.create(Schema.Type.INT))
    }
  }

}

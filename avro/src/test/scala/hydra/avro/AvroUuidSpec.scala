package hydra.avro

import hydra.avro.convert.AvroUuid
import org.apache.avro.Schema
import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpecLike

class AvroUuidSpec extends Matchers with AnyFlatSpecLike {

  "The AvroUuid class" should "validate correct types" in {
    AvroUuid.validate(Schema.create(Schema.Type.STRING))
    intercept[IllegalArgumentException] {
      AvroUuid.validate(Schema.create(Schema.Type.INT))
    }
  }

}

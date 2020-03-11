package hydra.kafka.producer

import hydra.core.transport.AckStrategy
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericRecordBuilder
import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpecLike

class AvroKeyRecordSpec extends AnyFlatSpecLike with Matchers {

  it must "construct an AvroKeyRecord" in {
    def schema(name: String) =
      SchemaBuilder
        .record(name)
        .fields()
        .name(name)
        .`type`
        .stringType()
        .noDefault()
        .endRecord()
    def json(name: String) =
      s"""
         |{
         |  "$name":"test"
         |}
         |""".stripMargin
    val avroKeyRecord = AvroKeyRecord.apply(
      "dest",
      schema("key"),
      schema("value"),
      json("key"),
      json("value"),
      AckStrategy.Replicated
    )
    def genRecord(name: String) =
      new GenericRecordBuilder(schema(name)).set(name, "test").build()

    avroKeyRecord shouldBe AvroKeyRecord(
      "dest",
      schema("key"),
      schema("value"),
      genRecord("key"),
      genRecord("value"),
      AckStrategy.Replicated
    )
  }

}

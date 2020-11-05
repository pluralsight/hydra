package hydra.avro.convert

import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericRecordBuilder
import org.apache.avro.util.Utf8
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

final class SimpleStringToGenericRecordSpec extends AnyFlatSpec with Matchers {

  import SimpleStringToGenericRecord._

  it should "convert basic record" in {
    val schema = SchemaBuilder.record("Test").fields()
      .requiredString("testing").endRecord()
    val json =
      """
        |{"testing": "test"}
        |""".stripMargin
    val record = json.toGenericRecordSimple(schema, useStrictValidation = true)
    record.get.get("testing") shouldBe new Utf8("test")
  }

  it should "convert basic union type with null as one member" in {
    val schema = SchemaBuilder.record("Test").fields()
      .optionalString("testing").endRecord()
    val json =
      """
        |{"testing": "test"}
        |""".stripMargin
    val record = json.toGenericRecordSimple(schema, useStrictValidation = true)
    record.get.get("testing") shouldBe new Utf8("test")
  }

  it should "convert basic union type containing a record and null" in {
    val innerSchema = SchemaBuilder.record("InnerTest")
      .fields().optionalString("blah").endRecord()
    val schema = SchemaBuilder.record("Test").fields()
      .name("testing")
      .`type`(innerSchema).noDefault().endRecord()
    val json =
      """
        |{
        |  "testing": {
        |    "blah": "test"
        |  }
        |}
        |""".stripMargin
    val record = json.toGenericRecordSimple(schema, useStrictValidation = true)
    record.get.get("testing") shouldBe new GenericRecordBuilder(innerSchema).set("blah", "test").build()
  }

  it should "allow the null branch of the union to be specified" in {
    val schema = SchemaBuilder.record("Test").fields()
      .optionalString("testing").endRecord()
    val json =
      """
        |{"testing": null}
        |""".stripMargin
    val record = json.toGenericRecordSimple(schema, useStrictValidation = true)
    record.get.get("testing") shouldBe null
  }

  it should "imply the null branch of the union if the field is missing" in {
    val schema = SchemaBuilder.record("Test").fields()
      .optionalString("testing").endRecord()
    val json =
      """
        |{}
        |""".stripMargin
    val record = json.toGenericRecordSimple(schema, useStrictValidation = true)
    record.get.get("testing") shouldBe null
  }

}

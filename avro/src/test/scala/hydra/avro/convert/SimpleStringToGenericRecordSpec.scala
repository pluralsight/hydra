package hydra.avro.convert

import java.nio.ByteBuffer

import hydra.avro.convert.StringToGenericRecord.ValidationExtraFieldsError
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.generic.{GenericData, GenericRecord, GenericRecordBuilder}
import org.apache.avro.util.Utf8
import org.apache.kafka.common.utils.Bytes
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import spray.json.{JsArray, JsObject, JsString}

import scala.util.Failure

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

  it should "convert basic enum record" in {
    val schema = SchemaBuilder.record("Test").fields()
      .name("testing").`type`.enumeration("testEnum").symbols("ONE", "TWO").noDefault().endRecord()
    val json =
      """
        |{"testing": "ONE"}
        |""".stripMargin
    val record = json.toGenericRecordSimple(schema, useStrictValidation = true)
    record.get.get("testing").toString shouldBe "ONE"
  }

  it should "raise error when converting record with wrong type" in {
    val schema = SchemaBuilder.record("Test").fields()
      .name("testing").`type`.array.items.stringType.noDefault().endRecord()
    val json =
      """
        |"testing"
        |""".stripMargin
    val record = json.toGenericRecordSimple(schema, useStrictValidation = true)
    record shouldBe Failure(UnexpectedTypeFoundInGenericRecordConversion[JsObject](classOf[JsObject], JsString("testing")))
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

  it should "handle union with only null type" in {
    val schema = SchemaBuilder.record("Test").fields()
      .name("testing").`type`.unionOf().nullType().endUnion().noDefault().endRecord()
    val json =
      """
        |{
        |  "testing": null
        |}
        |""".stripMargin
    val record = json.toGenericRecordSimple(schema, useStrictValidation = true)
    Option(record.get.get("testing")) shouldBe None
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

  it should "convert array of simple type" in {
    val schema = SchemaBuilder.record("Test").fields()
      .name("testing").`type`.array.items.stringType.noDefault().endRecord()
    val json =
      """
        |{"testing": ["one", "two", "three"]}
        |""".stripMargin
    val record = json.toGenericRecordSimple(schema, useStrictValidation = true)
    import collection.JavaConverters._
    record.get.get("testing") match {
      case g: GenericData.Array[_] => g.toArray shouldBe List("one", "two", "three").map(new Utf8(_)).asJava.toArray
      case other => fail(s"$other not a recognized type")
    }
  }

  it should "raise error when converting array with wrong type" in {
    val schema = SchemaBuilder.record("Test").fields()
      .name("testing").`type`.array.items.stringType.noDefault().endRecord()
    val json =
      """
        |{
        |  "testing": "fail"
        |}
        |""".stripMargin
    val record = json.toGenericRecordSimple(schema, useStrictValidation = true)
    record shouldBe Failure(UnexpectedTypeFoundInGenericRecordConversion[JsArray](classOf[JsArray], JsString("fail")))
  }

  it should "convert array of records" in {
    val innerSchema = SchemaBuilder.record("InnerTest")
      .fields().optionalString("blah").endRecord()
    val schema = SchemaBuilder.record("Test").fields()
      .name("testing").`type`.array.items(innerSchema).noDefault().endRecord()
    val json =
      """
        |{
        |  "testing": [
        |    {
        |      "blah": "one"
        |    },
        |    {
        |      "blah": "two"
        |    },
        |    {
        |      "blah": "three"
        |    }
        |  ]
        |}
        |""".stripMargin
    val record = json.toGenericRecordSimple(schema, useStrictValidation = true)
    import collection.JavaConverters._
    val toGenRecord: String => GenericRecord = str => new GenericRecordBuilder(innerSchema).set("blah", new Utf8(str)).build()
    record.get.get("testing") match {
      case g: GenericData.Array[_] => g.toArray shouldBe List("one", "two", "three").map(toGenRecord).asJava.toArray
      case other => fail(s"$other not a recognized type")
    }
  }

  it should "convert map of simple type" in {
    val schema = SchemaBuilder.record("Test").fields()
      .name("testing").`type`.map.values.intType.noDefault().endRecord()
    val json =
      """
        |{
        |  "testing": {
        |    "one": 1,
        |    "two": 2,
        |    "three": 3
        |  }
        |}
        |""".stripMargin
    val record = json.toGenericRecordSimple(schema, useStrictValidation = true)
    import collection.JavaConverters._
    record.get.get("testing") shouldBe Map("one" -> 1, "two" -> 2, "three" -> 3).map(kv => new Utf8(kv._1) -> kv._2).asJava
  }

  it should "raise error when converting map with wrong type" in {
    val schema = SchemaBuilder.record("Test").fields()
      .name("testing").`type`.map.values.intType.noDefault().endRecord()
    val json =
      """
        |{
        |  "testing": "fail"
        |}
        |""".stripMargin
    val record = json.toGenericRecordSimple(schema, useStrictValidation = true)
    record shouldBe Failure(UnexpectedTypeFoundInGenericRecordConversion[JsObject](classOf[JsObject], JsString("fail")))
  }

  it should "convert map of complex type" in {
    val innerSchema = SchemaBuilder.record("InnerTest")
      .fields().optionalString("blah").endRecord()
    val schema = SchemaBuilder.record("Test").fields()
      .name("testing").`type`.map.values.`type`(innerSchema).noDefault().endRecord()
    val json =
      """
        |{
        |  "testing": {
        |    "one": {"blah": "one"},
        |    "two": {"blah": "two"},
        |    "three": {"blah": "three"}
        |  }
        |}
        |""".stripMargin
    val record = json.toGenericRecordSimple(schema, useStrictValidation = true)
    import collection.JavaConverters._
    val toGenRecord: String => GenericRecord = str => new GenericRecordBuilder(innerSchema).set("blah", new Utf8(str)).build()
    record.get.get("testing") shouldBe Map("one" -> "one", "two" -> "two", "three" -> "three")
      .map(kv => new Utf8(kv._1) -> kv._2).mapValues(toGenRecord).asJava
  }

  it should "throw a Strict Validation Error if Json contains additional fields" in {
    val schema = SchemaBuilder.record("Strict").fields().requiredString("id").optionalBoolean("boolOpt").endRecord()
    val json = """{"id":"123abc","additionalField":false}"""
    val record = json.toGenericRecordSimple(schema, useStrictValidation = true)
    record shouldBe a[Failure[ValidationExtraFieldsError]]
  }

  it should "Use the Schema default for string, boolean, bytes, double, float, int, long, null, enum, and fixed if no json value is provided" in {
    val schema = SchemaBuilder.record("SchemaDefaults").fields()
      .name("string1").`type`().stringType().stringDefault("helloThere")
      .name("bool1").`type`().booleanType().booleanDefault(true)
      .name("bytes1").`type`().bytesType().bytesDefault(Array[Byte](2, 4, 1, 9))
      .name("double1").`type`().doubleType().doubleDefault(123.45)
      .name("float1").`type`().floatType().floatDefault(123.45F)
      .name("int1").`type`().intType().intDefault(123)
      .name("long1").`type`().longType().longDefault(123L)
      .name("null1").`type`().nullType().nullDefault()
      .name("enum1").`type`().enumeration("asf").symbols("FOOTBALL","HOCKEY","GOLF").enumDefault("GOLF")
      .name("fixed1").`type`().fixed("fixedValue").size(4).fixedDefault(Array[Byte](2, 4, 1, 9))
      .endRecord()
    val json = """{}"""
    val record = json.toGenericRecordSimple(schema).get
    record.get("string1") shouldBe new Utf8("helloThere")
    record.get("bool1") shouldBe true
    record.get("bytes1").asInstanceOf[ByteBuffer].array() shouldBe Array[Byte](2, 4, 1, 9)
    record.get("double1") shouldBe 123.45
    record.get("float1") shouldBe 123.45F
    record.get("int1") shouldBe 123
    record.get("long1") shouldBe 123L
    record.get("null1") shouldBe null
    record.get("enum1") shouldBe new GenericData.EnumSymbol(record.getSchema, "GOLF")
    record.get("fixed1") shouldBe new GenericData.Fixed(record.getSchema, Array[Byte](2, 4, 1, 9))
  }

  it should "Use the Schema default for string if no json value is provided" in {
    val schema = SchemaBuilder.record("SchemaDefaults").fields().requiredString("id").name("string1").`type`().stringType().stringDefault("helloThere").endRecord()
    val json = """{"id":"123"}"""
    val record = json.toGenericRecordSimple(schema, useStrictValidation = true)
    record.get.get("string1") shouldBe new Utf8("helloThere")
  }

  it should "Use the default for a subrecord when no json value is provided" in {
    val innerSchema = SchemaBuilder.record("innerRecord").fields().requiredInt("intOpt").endRecord()
    val genericRecord: GenericData.Record = new GenericRecordBuilder(innerSchema).set("intOpt", 12).build()
    val schema = SchemaBuilder.record("SchemaDefaults").fields().name("subrecord").`type`().record("innerRecord").fields().requiredInt("intOpt").endRecord().recordDefault(genericRecord).endRecord()
    val json = """{}"""
    val record = json.toGenericRecordSimple(schema, useStrictValidation = true)
    record.get.get("subrecord") shouldBe new GenericRecordBuilder(innerSchema).set("intOpt", 12).build()
  }

  it should "verify that"

  it should "Return null when no default is provided" in {
    val schema = SchemaBuilder.record("SchemaDefaults").fields().requiredString("id").optionalBoolean("bool1").endRecord()
    val json = """{"id":"123"}"""
    val record = json.toGenericRecordSimple(schema, useStrictValidation = true)
    record.get.get("bool1") shouldBe null
  }

}

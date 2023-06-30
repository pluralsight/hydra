package hydra.avro.convert

import java.time.Instant
import java.util.UUID
import org.apache.avro.{AvroTypeException, LogicalTypes, Schema, SchemaBuilder}
import org.apache.avro.util.Utf8
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.{Failure, Success}

final class StringToGenericRecordSpec extends AnyFlatSpec with Matchers {

  import StringToGenericRecord._

  /*
  These tests do not need to exhaustively test successful JSON to GenericRecord translation since another library is
  doing that. These just need to test that we are handling Strict and Relaxed validation types.
   */

  private val timestampSchema = SchemaBuilder.record("testVal")
    .fields()
    .name("testTimestamp")
    .`type`(LogicalTypes.timestampMillis.addToSchema(Schema.create(Schema.Type.LONG)))
    .noDefault
    .endRecord

  it should "convert basic record" in {
    val schema = SchemaBuilder.record("Test").fields()
      .requiredString("testing").endRecord()
    val record = """{"testing": "test"}""".toGenericRecord(schema, useStrictValidation = true, useTimestampValidation = false)
    record.get.get("testing") shouldBe new Utf8("test")
  }

  it should "convert union record" in {
    val schema = SchemaBuilder.record("Test").fields()
      .optionalString("testing").endRecord()
    val record = """{"testing": {"string": "test"}}""".toGenericRecord(schema, useStrictValidation = true, useTimestampValidation = false)
    record.get.get("testing") shouldBe new Utf8("test")
  }

  it should "convert union record with explicit null branch" in {
    val schema = SchemaBuilder.record("Test").fields()
      .optionalString("testing").endRecord()
    val record = """{"testing": {"null": null}}""".toGenericRecord(schema, useStrictValidation = true, useTimestampValidation = false)
    record.get.get("testing") shouldBe null
  }

  it should "convert union record with implicit null branch" in {
    val schema = SchemaBuilder.record("Test").fields()
      .optionalString("testing").endRecord()
    val record = """{"testing": null}""".toGenericRecord(schema, useStrictValidation = true, useTimestampValidation = false)
    record.get.get("testing") shouldBe null
  }

  it should "convert union record with inner record with implicit null branch" in {
    val schema = SchemaBuilder.record("Test").namespace("my.namespace").fields().name("testing").`type`()
      .unionOf().nullType().and().record("TestingInner").fields()
      .requiredInt("testInner").endRecord().endUnion().nullDefault().endRecord()
    val record =
    """{"testing": {"my.namespace.TestingInner": {"testInner": 2020}}}""".toGenericRecord(schema, useStrictValidation = true, useTimestampValidation = false)
    record.get.get("testing").toString shouldBe "{\"testInner\": 2020}"
  }

  it should "reject union record with explicit null branch containing extra fields" in {
    val schema = SchemaBuilder.record("Test").fields()
      .optionalString("testing").endRecord()
    val record = """{"testing": {"null": null, "another": 2020}}""".toGenericRecord(schema, useStrictValidation = true, useTimestampValidation = false)
    record shouldBe a[Failure[_]]
  }

  it should "return an error for extra field and Strict validation" in {
    val schema = SchemaBuilder.record("Test").fields()
      .requiredString("testing").endRecord()
    val record = """{"testing": "test", "blah": 10}""".toGenericRecord(schema, useStrictValidation = true, useTimestampValidation = false)
    record shouldBe a[Failure[_]]
  }

  it should "return valid in nested record and Strict validation" in {
    val inner = SchemaBuilder.record("Test").fields()
      .requiredInt("testInner").endRecord()
    val schema = SchemaBuilder.record("Test").fields()
      .requiredString("testing").name("nested").`type`(inner).noDefault.endRecord()
    val record = """{"testing": "test", "nested": {"testInner": 10}}""".toGenericRecord(schema, useStrictValidation = true, useTimestampValidation = false)
    record shouldBe a[Success[_]]
  }

  it should "return error in nested record with extra field and Strict validation" in {
    val inner = SchemaBuilder.record("Test").fields()
      .requiredInt("testInner").endRecord()
    val schema = SchemaBuilder.record("Test").fields()
      .requiredString("testing").name("nested").`type`(inner).noDefault.endRecord()
    val record =
    """{"testing": "test", "nested": {"testInner": 10, "blah": 90}}""".toGenericRecord(schema, useStrictValidation = true, useTimestampValidation = false)
    record shouldBe a[Failure[_]]
  }

  it should "return success in nested record with extra field and Relaxed validation" in {
    val inner = SchemaBuilder.record("Test").fields()
      .requiredInt("testInner").endRecord()
    val schema = SchemaBuilder.record("Test").fields()
      .requiredString("testing").name("nested").`type`(inner).noDefault.endRecord()
    val record =
    """{"testing": "test", "nested": {"testInner": 10, "blah": 90}}""".toGenericRecord(schema, useStrictValidation = false, useTimestampValidation = false)
    record shouldBe a[Success[_]]
  }

  it should "return error in nested record of same name with extra field and Strict validation" in {
    val inner = SchemaBuilder.record("Test").fields()
      .requiredInt("testInner").endRecord()
    val schema = SchemaBuilder.record("Test").fields()
      .requiredString("testing").name("nested").`type`(inner).noDefault.endRecord()
    val record =
    """{"testing": "test", "nested": {"testInner": 10, "testing": 90}}""".toGenericRecord(schema, useStrictValidation = true, useTimestampValidation = false)
    record shouldBe a[Failure[_]]
  }

  it should "return error if record contains two of same field in same scope" in {
    val schema = SchemaBuilder.record("Test").fields()
      .requiredInt("testing").endRecord()
    val record = """{"testing": "test", "testing": "test"}""".toGenericRecord(schema, useStrictValidation = false, useTimestampValidation = false)
    record shouldBe a[Failure[_]]
  }

  it should "validate UUID logical type" in {
    val schema = SchemaBuilder.record("testVal")
      .fields()
      .name("testUuid")
      .`type`(LogicalTypes.uuid.addToSchema(Schema.create(Schema.Type.STRING)))
      .noDefault
      .endRecord
    val record = """{"testUuid": "test"}""".toGenericRecord(schema, useStrictValidation = true, useTimestampValidation = false)
    record shouldBe a[Failure[_]]
  }

  it should "accept a valid UUID logical type" in {
    val uuid = UUID.randomUUID
    val schema = SchemaBuilder.record("testVal")
      .fields()
      .name("testUuid")
      .`type`(LogicalTypes.uuid.addToSchema(Schema.create(Schema.Type.STRING)))
      .noDefault
      .endRecord
    val record = s"""{"testUuid": "${uuid.toString}"}""".toGenericRecord(schema, useStrictValidation = true, useTimestampValidation = false)
    record shouldBe a[Success[_]]
  }

  it should "reject a bad nested UUID logical type" in {
    val inner = SchemaBuilder.record("testVal")
      .fields()
      .name("testUuid")
      .`type`(LogicalTypes.uuid.addToSchema(Schema.create(Schema.Type.STRING)))
      .noDefault
      .endRecord
    val schema = SchemaBuilder.record("testOuterVal")
      .fields()
      .requiredBoolean("extra")
      .name("inner")
      .`type`(inner)
      .noDefault
      .endRecord
    val record = """{"extra": true, "inner": {"testUuid": "test"}}""".toGenericRecord(schema, useStrictValidation = true, useTimestampValidation = false)
    record shouldBe a[Failure[_]]
  }

  it should "accept a good nested UUID logical type" in {
    val uuid = UUID.randomUUID
    val inner = SchemaBuilder.record("testVal")
      .fields()
      .name("testUuid")
      .`type`(LogicalTypes.uuid.addToSchema(Schema.create(Schema.Type.STRING)))
      .noDefault
      .endRecord
    val schema = SchemaBuilder.record("testOuterVal")
      .fields()
      .requiredBoolean("extra")
      .name("inner")
      .`type`(inner)
      .noDefault
      .endRecord
    val record =
      s"""{"extra": true, "inner": {"testUuid": "${uuid.toString}"}}""".toGenericRecord(schema, useStrictValidation = true, useTimestampValidation = false)
    record shouldBe a[Success[_]]
  }

  it should "validate TimestampMillis logical type" in {
    val schema = SchemaBuilder.record("testVal")
      .fields()
      .name("testTs")
      .`type`(LogicalTypes.timestampMillis.addToSchema(Schema.create(Schema.Type.LONG)))
      .noDefault
      .endRecord
    val record = """{"testTs": 819283928392839283928398293829382938298329839283928392839283928398}""".
      toGenericRecord(schema, useStrictValidation = true, useTimestampValidation = false)
    record shouldBe a[Failure[_]]
  }

  it should "accept a valid TimestampMillis logical type" in {
    val ts = Instant.now
    val schema = SchemaBuilder.record("testVal")
      .fields()
      .name("testTs")
      .`type`(LogicalTypes.timestampMillis.addToSchema(Schema.create(Schema.Type.LONG)))
      .noDefault
      .endRecord
    val record = s"""{"testTs": ${ts.toEpochMilli}}""".toGenericRecord(schema, useStrictValidation = true, useTimestampValidation = false)
    record shouldBe a[Success[_]]
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
    val record = json.toGenericRecord(schema, useStrictValidation = true, useTimestampValidation = false)
    import collection.JavaConverters._
    record.get.get("testing") shouldBe Map("one" -> 1, "two" -> 2, "three" -> 3).map(kv => new Utf8(kv._1) -> kv._2).asJava
  }

  it should "throw an AvroTypeException for field testInner with unexpected value" in {
    val inner = SchemaBuilder.record("Test").fields()
      .requiredInt("testInner2").endRecord()
    val inner2 = SchemaBuilder.record("Test").fields()
      .name("testInner").`type`(inner).noDefault.endRecord()
    val schema = SchemaBuilder.record("Test").fields()
      .requiredString("testing").name("nested").`type`(inner2).noDefault.endRecord()

    the[AvroTypeException] thrownBy """{"testing": "test", "nested": {"testInner": {"testInner2" : "10"}}}""".
      toGenericRecord(schema, useStrictValidation = true, useTimestampValidation = false).get should have message "nested -> testInner -> testInner2 -> Expected int. Got VALUE_STRING"
  }

  it should "throw an AvroTypeException for union field with unexpected value" in {
    val innerSchema = SchemaBuilder.record("Test").namespace("my.namespace").fields().name("testing").`type`()
      .unionOf().nullType().and().record("TestingInner").fields()
      .requiredInt("testInner").endRecord().endUnion().nullDefault().endRecord()

    val schema = SchemaBuilder.record("nested").fields()
      .requiredString("testing2").name("nested").`type`(innerSchema).noDefault.endRecord()

    val json = """{"testing2": "test", "nested": {"testing": {"my.namespace.TestingInner": {"testInner": "2020"}}}}"""

    the[AvroTypeException] thrownBy json.
      toGenericRecord(schema, useStrictValidation = true, useTimestampValidation = false).get should have message "nested -> testInner -> Expected int. Got VALUE_STRING"

    the[AvroTypeException] thrownBy """{"testing": {"my.namespace.TestingInner": {"testInner": "2020"}}}""".
      toGenericRecord(innerSchema, useStrictValidation = true, useTimestampValidation = false).get should have message "testInner -> Expected int. Got VALUE_STRING"
  }

  it should "not throw an AvroTypeException for union field with null value" in {
    val schema = SchemaBuilder.record("Test").namespace("my.namespace").fields().name("testing").`type`()
      .unionOf().nullType().and().record("TestingInner").fields()
      .requiredInt("testInner").endRecord().endUnion().nullDefault().endRecord()

    val record =
    """{"testing": {"my.namespace.TestingInner": {"testInner": 2020}}}""".toGenericRecord(schema, useStrictValidation = true, useTimestampValidation = false)
    record shouldBe a[Success[_]]
  }

  it should "throw an AvroTypeException for field with logical type and unexpected value" in {
    val ts = Instant.now
    val schema = SchemaBuilder.record("testVal")
      .fields()
      .name("testTs")
      .`type`(LogicalTypes.timestampMillis.addToSchema(Schema.create(Schema.Type.LONG)))
      .noDefault
      .endRecord

    the[AvroTypeException] thrownBy s"""{"testTs": "hi"}""".
      toGenericRecord(schema, useStrictValidation = true, useTimestampValidation = false).get should have message "testTs -> Expected long. Got VALUE_STRING"
  }

  it should "throw an InvalidLogicalTypeErrorForTimeStamp for field with logical type of timestamp-millis having value 0 with timestampValidation enabled" in {
    the[InvalidLogicalTypeErrorForTimeStamp] thrownBy
      s"""{"testTimestamp": 0}""".
        toGenericRecord(timestampSchema, useStrictValidation = true, useTimestampValidation = true).get should have message
      "Invalid value for field 'testTimestamp' having logical type - timestamp-millis. Value should be greater than 0 but received 0"
  }

  it should "throw an InvalidLogicalTypeErrorForTimeStamp for field with logical type of timestamp-millis having value -2 with timestampValidation enabled" in {
    the[InvalidLogicalTypeErrorForTimeStamp] thrownBy
      s"""{"testTimestamp": -2}""".
        toGenericRecord(timestampSchema, useStrictValidation = true, useTimestampValidation = true).get should have message
      "Invalid value for field 'testTimestamp' having logical type - timestamp-millis. Value should be greater than 0 but received -2"
  }

  it should "accept a logical field type of timestamp-millis having a value 0 with timestampValidation disabled" in {
    val record = """{"testTimestamp": 0}""".toGenericRecord(timestampSchema, useStrictValidation = true, useTimestampValidation = false)
    record shouldBe a[Success[_]]
  }

  it should "accept a logical field type of timestamp-millis having negative value -2 with timestampValidation disabled" in {
    val record = """{"testTimestamp": -2}""".toGenericRecord(timestampSchema, useStrictValidation = true, useTimestampValidation = false)
    record shouldBe a[Success[_]]
  }
}

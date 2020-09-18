package hydra.avro.convert

import java.time.Instant
import java.util.UUID

import org.apache.avro.{LogicalTypes, Schema, SchemaBuilder}
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

  it should "convert basic record" in {
    val schema = SchemaBuilder.record("Test").fields()
      .requiredString("testing").endRecord()
    val record = """{"testing": "test"}""".toGenericRecord(schema, useStrictValidation = true)
    record.get.get("testing") shouldBe new Utf8("test")
  }

  it should "return an error for extra field and Strict validation" in {
    val schema = SchemaBuilder.record("Test").fields()
      .requiredString("testing").endRecord()
    val record = """{"testing": "test", "blah": 10}""".toGenericRecord(schema, useStrictValidation = true)
    record shouldBe a[Failure[_]]
  }

  it should "return valid in nested record and Strict validation" in {
    val inner = SchemaBuilder.record("Test").fields()
      .requiredInt("testInner").endRecord()
    val schema = SchemaBuilder.record("Test").fields()
      .requiredString("testing").name("nested").`type`(inner).noDefault.endRecord()
    val record = """{"testing": "test", "nested": {"testInner": 10}}""".toGenericRecord(schema, useStrictValidation = true)
    record shouldBe a[Success[_]]
  }

  it should "return valid for union and Strict validation" in {
    val schema = SchemaBuilder.record("Test").fields().optionalString("myString").endRecord()
    val record = """{"myString": {"string": "testtesttest"}}""".toGenericRecord(schema, useStrictValidation = true)
    record shouldBe a[Success[_]]
  }

  it should "return error in nested record with extra field and Strict validation" in {
    val inner = SchemaBuilder.record("Test").fields()
      .requiredInt("testInner").endRecord()
    val schema = SchemaBuilder.record("Test").fields()
      .requiredString("testing").name("nested").`type`(inner).noDefault.endRecord()
    val record = """{"testing": "test", "nested": {"testInner": 10, "blah": 90}}""".toGenericRecord(schema, useStrictValidation = true)
    record shouldBe a[Failure[_]]
  }

  it should "return success in nested record with extra field and Relaxed validation" in {
    val inner = SchemaBuilder.record("Test").fields()
      .requiredInt("testInner").endRecord()
    val schema = SchemaBuilder.record("Test").fields()
      .requiredString("testing").name("nested").`type`(inner).noDefault.endRecord()
    val record = """{"testing": "test", "nested": {"testInner": 10, "blah": 90}}""".toGenericRecord(schema, useStrictValidation = false)
    record shouldBe a[Success[_]]
  }

  it should "return error in nested record of same name with extra field and Strict validation" in {
    val inner = SchemaBuilder.record("Test").fields()
      .requiredInt("testInner").endRecord()
    val schema = SchemaBuilder.record("Test").fields()
      .requiredString("testing").name("nested").`type`(inner).noDefault.endRecord()
    val record = """{"testing": "test", "nested": {"testInner": 10, "testing": 90}}""".toGenericRecord(schema, useStrictValidation = true)
    record shouldBe a[Failure[_]]
  }

  it should "return error if record contains two of same field in same scope" in {
    val schema = SchemaBuilder.record("Test").fields()
      .requiredInt("testing").endRecord()
    val record = """{"testing": "test", "testing": "test"}""".toGenericRecord(schema, useStrictValidation = false)
    record shouldBe a[Failure[_]]
  }

  it should "validate UUID logical type" in {
    val schema = SchemaBuilder.record("testVal")
      .fields()
      .name("testUuid")
      .`type`(LogicalTypes.uuid.addToSchema(Schema.create(Schema.Type.STRING)))
      .noDefault
      .endRecord
    val record = """{"testUuid": "test"}""".toGenericRecord(schema, useStrictValidation = true)
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
    val record = s"""{"testUuid": "${uuid.toString}"}""".toGenericRecord(schema, useStrictValidation = true)
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
    val record = """{"extra": true, "inner": {"testUuid": "test"}}""".toGenericRecord(schema, useStrictValidation = true)
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
    val record = s"""{"extra": true, "inner": {"testUuid": "${uuid.toString}"}}""".toGenericRecord(schema, useStrictValidation = true)
    record shouldBe a[Success[_]]
  }

  it should "validate TimestampMillis logical type" in {
    val schema = SchemaBuilder.record("testVal")
      .fields()
      .name("testTs")
      .`type`(LogicalTypes.timestampMillis.addToSchema(Schema.create(Schema.Type.LONG)))
      .noDefault
      .endRecord
    val record = """{"testTs": 819283928392839283928398293829382938298329839283928392839283928398}""".toGenericRecord(schema, useStrictValidation = true)
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
    val record = s"""{"testTs": ${ts.toEpochMilli}}""".toGenericRecord(schema, useStrictValidation = true)
    record shouldBe a[Success[_]]
  }

}

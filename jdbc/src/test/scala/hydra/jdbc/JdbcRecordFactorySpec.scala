package hydra.jdbc

import com.pluralsight.hydra.avro.JsonConverter
import hydra.avro.JsonToAvroConversionExceptionWithMetadata
import hydra.core.ingest.HydraRequest
import hydra.core.ingest.RequestParams.HYDRA_SCHEMA_PARAM
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.scalatest.{FunSpecLike, Matchers}

import scala.io.Source

class JdbcRecordFactorySpec extends Matchers with FunSpecLike {
  val schemaPK = new Schema.Parser().parse(Source.fromResource("schemaPK.avsc").mkString)
  val schema = new Schema.Parser().parse(Source.fromResource("schema.avsc").mkString)

  describe("The JDBC record factory") {
    it("extracts primary keys if present") {
      val req = HydraRequest(1, "test")
      JdbcRecordFactory.pk(req, schemaPK) shouldBe Seq(schemaPK.getField("id"))

      val req1 = HydraRequest(1, "test")
      JdbcRecordFactory.pk(req1, schema) shouldBe Seq.empty

      val req2 = HydraRequest(1, "test").withMetadata(JdbcRecordFactory.PRIMARY_KEY_PARAM -> "id")
      JdbcRecordFactory.pk(req2, schema) shouldBe Seq(schema.getField("id"))

      val req3 = HydraRequest(1, "test").withMetadata(JdbcRecordFactory.PRIMARY_KEY_PARAM -> "unknown")
      intercept[IllegalArgumentException] {
        JdbcRecordFactory.pk(req3, schema) shouldBe Seq(schema.getField("id"))
      }
    }

    it("throws an error if no schema is in the request metadata") {
      val req = HydraRequest(1, "test")
      intercept[IllegalArgumentException] {
        JdbcRecordFactory.build(req).get
      }
    }

    it("throws an error if payload does not comply to schema") {
      val request = HydraRequest(123,"""{"name":"test"}""")
        .withMetadata(HYDRA_SCHEMA_PARAM -> "classpath:schema.avsc")
      intercept[JsonToAvroConversionExceptionWithMetadata] {
        JdbcRecordFactory.build(request).get
      }
    }

    it("throws an error if payload if validation is strict") {
      val request = HydraRequest(123,"""{"id":1, "field":2, "name":"test"}""")
        .withMetadata(HYDRA_SCHEMA_PARAM -> "classpath:schema.avsc")
      intercept[JsonToAvroConversionExceptionWithMetadata] {
        JdbcRecordFactory.build(request).get
      }
    }

    it("Uses the schema as the table name") {
      val request = HydraRequest(123,"""{"id":1, "name":"test", "rank" : 1}""")
        .withMetadata(HYDRA_SCHEMA_PARAM -> "classpath:schema.avsc", JdbcRecordFactory.DB_PROFILE_PARAM -> "table")

      JdbcRecordFactory.build(request).get.destination shouldBe schema.getName

    }

    it("throws an error if no db profile is present in the request") {
      val request = HydraRequest(123,"""{"id":1, "name":"test", "rank" : 1}""")
        .withMetadata(HYDRA_SCHEMA_PARAM -> "classpath:schema.avsc", JdbcRecordFactory.TABLE_PARAM -> "table")
      intercept[IllegalArgumentException] {
        JdbcRecordFactory.build(request).get
      }
    }

    it("builds a record without a PK") {
      val request = HydraRequest(123,"""{"id":1, "name":"test", "rank" : 1}""")
        .withMetadata(HYDRA_SCHEMA_PARAM -> "classpath:schema.avsc",
          JdbcRecordFactory.TABLE_PARAM -> "table", JdbcRecordFactory.DB_PROFILE_PARAM -> "table")

      val rec = JdbcRecordFactory.build(request).get
      rec.destination shouldBe "table"
      rec.key shouldBe Some(Seq.empty)
      rec.payload shouldBe new JsonConverter[GenericRecord](schema).convert("""{"id":1, "name":"test", "rank" : 1}""")
    }

    it("builds a record with a PK") {
      val request = HydraRequest(123,"""{"id":1, "name":"test", "rank" : 1}""")
        .withMetadata(HYDRA_SCHEMA_PARAM -> "classpath:schemaPK.avsc", JdbcRecordFactory.DB_PROFILE_PARAM -> "table")

      val rec = JdbcRecordFactory.build(request).get
      rec.destination shouldBe schemaPK.getName
      rec.key shouldBe Some(Seq(schemaPK.getField("id")))
      rec.payload shouldBe new JsonConverter[GenericRecord](schemaPK).convert("""{"id":1, "name":"test", "rank" : 1}""")
    }
  }
}

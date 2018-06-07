package hydra.kafka.producer

import com.pluralsight.hydra.avro.JsonConverter
import hydra.core.ingest.{HydraRequest, RequestParams}
import hydra.kafka.producer.KafkaRecordFactory.RecordKeyExtractor.SchemaKeyExtractor
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.scalatest.{FlatSpecLike, Matchers}

class SchemaKeyExtractorSpec extends Matchers with FlatSpecLike {

  private def buildRecord(schemaStr: String, payload: String) = {
    val schema = new Schema.Parser().parse(schemaStr)
    new JsonConverter[GenericRecord](schema).convert(payload)
  }

  "The SchemaKeyExtractor" should "return none when no key is present" in {
    val record = buildRecord(
      """
        |{
        |  "namespace": "hydra.test",
        |  "type": "record",
        |  "name": "Tester",
        |  "fields": [
        |    {
        |      "name": "name",
        |      "type": "string"
        |    },
        |    {
        |      "name": "rank",
        |      "type": "int"
        |    }
        |  ]
        |}
      """.stripMargin,
      """{"name":"hydra","rank":1}""")

    val req = HydraRequest("123", """{"name":"hydra","rank":1}""")
    SchemaKeyExtractor.extractKey(req, record) shouldBe None
  }

  it should "return a key when one is in the request" in {
    val record = buildRecord(
      """
        |{
        |  "namespace": "hydra.test",
        |  "type": "record",
        |  "name": "Tester",
        |  "fields": [
        |    {
        |      "name": "name",
        |      "type": "string"
        |    },
        |    {
        |      "name": "rank",
        |      "type": "int"
        |    }
        |  ]
        |}
      """.stripMargin,
      """{"name":"hydra","rank":1}""")

    val req = HydraRequest("123", """{"name":"hydra","rank":1}""")
      .withMetadata(RequestParams.HYDRA_RECORD_KEY_PARAM -> "theKey")
    SchemaKeyExtractor.extractKey(req, record) shouldBe Some("theKey")
  }

  it should "return the key defined by 'hydra.key'" in {
    val record = buildRecord(
      """
        |{
        |  "namespace": "hydra.test",
        |  "type": "record",
        |  "name": "Tester",
        |  "hydra.key":"name",
        |  "fields": [
        |    {
        |      "name": "name",
        |      "type": "string"
        |    },
        |    {
        |      "name": "rank",
        |      "type": "int"
        |    }
        |  ]
        |}
      """.stripMargin,
      """{"name":"hydra","rank":1}""")

    val req = HydraRequest("123", """{"name":"hydra","rank":1}""")
    SchemaKeyExtractor.extractKey(req, record) shouldBe Some("hydra")
  }

  it should "return the request when both hydra-record-key and hydra.key are present" in {
    val record = buildRecord(
      """
        |{
        |  "namespace": "hydra.test",
        |  "type": "record",
        |  "name": "Tester",
        |  "hydra.key":"name",
        |  "fields": [
        |    {
        |      "name": "name",
        |      "type": "string"
        |    },
        |    {
        |      "name": "rank",
        |      "type": "int"
        |    }
        |  ]
        |}
      """.stripMargin,
      """{"name":"hydra","rank":1}""")

    val req = HydraRequest("123", """{"name":"hydra","rank":1}""")
      .withMetadata(RequestParams.HYDRA_RECORD_KEY_PARAM -> "theKey")
    SchemaKeyExtractor.extractKey(req, record) shouldBe Some("theKey")
  }

  it should "error when trying to use hydra.key for a non-existent key" in {
    val record = buildRecord(
      """
        |{
        |  "namespace": "hydra.test",
        |  "type": "record",
        |  "name": "Tester",
        |  "hydra.key":"unknownField",
        |  "fields": [
        |    {
        |      "name": "name",
        |      "type": "string"
        |    },
        |    {
        |      "name": "rank",
        |      "type": "int"
        |    }
        |  ]
        |}
      """.stripMargin,
      """{"name":"hydra","rank":1}""")

    val req = HydraRequest("123", """{"name":"hydra","rank":1}""")
    intercept[IllegalArgumentException] {
      SchemaKeyExtractor.extractKey(req, record) shouldBe Some("1")
    }
  }

  it should "return composite keys" in {
    val record = buildRecord(
      """
        |{
        |  "namespace": "hydra.test",
        |  "type": "record",
        |  "name": "Tester",
        |  "hydra.key":"name,rank",
        |  "fields": [
        |    {
        |      "name": "name",
        |      "type": "string"
        |    },
        |    {
        |      "name": "rank",
        |      "type": "int"
        |    }
        |  ]
        |}
      """.stripMargin,
      """{"name":"hydra","rank":1}""")

    val req = HydraRequest("123", """{"name":"hydra","rank":1}""")
    SchemaKeyExtractor.extractKey(req, record) shouldBe Some("hydra|1")
  }
}

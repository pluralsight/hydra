package hydra.avro.convert

import org.apache.avro.{LogicalType, LogicalTypes, Schema}
import org.scalatest.{FlatSpec, Matchers}

class UUIDConverterSpec extends FlatSpec with Matchers {

  LogicalTypes.register(UUID.getName, new LogicalTypes.LogicalTypeFactory {
    override def fromSchema(schema: Schema): LogicalType = UUID
  })


  it should "use the logical type from a schema" in {
    val schemaStr =
      """
        |{
        |  "namespace": "hydra.test",
        |  "type": "record",
        |  "name": "GUID",
        |  "fields": [
        |    {
        |      "name": "guid",
        |      "type":{
        |        "type": "string",
        |        "logicalType":"uuid"
        |      }
        |    }
        |  ]
        |}
      """.stripMargin

    val schema = new Schema.Parser().parse(schemaStr)
    schema.getField("guid").schema().getLogicalType.getName shouldBe UUID.getName
  }

  it should "validate the type" in {
    UUID.validate(Schema.create(Schema.Type.STRING))

    intercept[IllegalArgumentException] {
      UUID.validate(Schema.create(Schema.Type.INT))
    }
  }
}

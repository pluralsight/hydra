package hydra.avro.convert

import java.util.UUID

import org.apache.avro.{LogicalType, LogicalTypes, Schema}
import org.scalatest.{FlatSpec, Matchers}

class UUIDConverterSpec extends FlatSpec with Matchers {

  LogicalTypes.register(HydraUUID.LogicalTypeName, new LogicalTypes.LogicalTypeFactory {
    override def fromSchema(schema: Schema): LogicalType = HydraUUID
  })

  val uuidConverter = new UUIDConverter

  "A UUIDConverter" should "get the converted type" in {
    uuidConverter.getConvertedType shouldEqual classOf[UUID]
  }

  it should "get the logical type name" in {
    uuidConverter.getLogicalTypeName shouldEqual HydraUUID.LogicalTypeName
  }

  it should "convert a UUID from a `CharSequence`" in {
    val charSeq = "123e4567-e89b-42d3-a456-556642440000"
    uuidConverter.fromCharSequence(charSeq, Schema.create(Schema.Type.STRING), HydraUUID)
  }

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
        |        "logicalType":"hydra-uuid"
        |      }
        |    }
        |  ]
        |}
      """.stripMargin

    val schema = new Schema.Parser().parse(schemaStr)
    schema.getField("guid").schema().getLogicalType shouldBe HydraUUID
  }
}

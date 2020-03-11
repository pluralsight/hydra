package hydra.avro

import java.time.{Instant, ZoneOffset, ZonedDateTime}

import hydra.avro.convert.{ISODateConverter, IsoDate}
import org.apache.avro.LogicalTypes.LogicalTypeFactory
import org.apache.avro.{LogicalType, LogicalTypes, Schema}
import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpecLike

class ISODateConverterSpec extends Matchers with AnyFlatSpecLike {

  LogicalTypes.register(IsoDate.IsoDateLogicalTypeName, new LogicalTypeFactory {
    override def fromSchema(schema: Schema): LogicalType = IsoDate
  })

  "The ISODateConverter class" should "convert ISO dates" in {
    val c = new ISODateConverter()
    c.getConvertedType shouldBe classOf[ZonedDateTime]
    c.getLogicalTypeName shouldBe "iso-datetime"
    val dt = c.fromCharSequence(
      "2015-07-28T19:55:57.693217+00:00",
      Schema.create(Schema.Type.STRING),
      IsoDate
    )

    dt.getYear shouldBe 2015
    dt.getMonthValue shouldBe 7
    dt.getDayOfMonth shouldBe 28
    dt.getHour shouldBe 19
    dt.getMinute shouldBe 55
    dt.getSecond shouldBe 57
    dt.getNano shouldBe 693217000
  }

  it should "parse valid dates that don't contain an offset" in {
    val c = new ISODateConverter()
    c.getConvertedType shouldBe classOf[ZonedDateTime]
    c.getLogicalTypeName shouldBe "iso-datetime"
    val dt = c.fromCharSequence(
      "2015-07-28T19:55:57.693217",
      Schema.create(Schema.Type.STRING),
      IsoDate
    )

    dt.getYear shouldBe 2015
    dt.getMonthValue shouldBe 7
    dt.getDayOfMonth shouldBe 28
    dt.getHour shouldBe 19
    dt.getMinute shouldBe 55
    dt.getSecond shouldBe 57
    dt.getNano shouldBe 693217000
  }

  it should "parse valid dates that don't contain milliseconds" in {
    val c = new ISODateConverter()
    c.getConvertedType shouldBe classOf[ZonedDateTime]
    c.getLogicalTypeName shouldBe "iso-datetime"
    val dt = c.fromCharSequence(
      "2019-09-18T02:44:32+0000",
      Schema.create(Schema.Type.STRING),
      IsoDate
    )

    dt.getYear shouldBe 2019
    dt.getMonthValue shouldBe 9
    dt.getDayOfMonth shouldBe 18
    dt.getHour shouldBe 2
    dt.getMinute shouldBe 44
    dt.getSecond shouldBe 32
    dt.getNano shouldBe 0
  }

  it should "return the epoch on bad formed dates" in {
    val c = new ISODateConverter()
    c.fromCharSequence(
      "2015-07-281",
      Schema.create(Schema.Type.STRING),
      IsoDate
    ) shouldBe Instant.EPOCH.atZone(ZoneOffset.UTC)
  }

  it should "use the logical type when parsing a schema" in {

    val schemaStr =
      """
        |{
        |  "namespace": "hydra.test",
        |  "type": "record",
        |  "name": "Date",
        |  "fields": [
        |    {
        |      "name": "timestamp",
        |      "type":{
        |        "type": "string",
        |        "logicalType":"iso-datetime"
        |      }
        |    }
        |  ]
        |}
      """.stripMargin

    val schema = new Schema.Parser().parse(schemaStr)
    schema.getField("timestamp").schema().getLogicalType shouldBe IsoDate
  }

  "The IsoDate LogicalType" should "validate correct types" in {
    IsoDate.validate(Schema.create(Schema.Type.STRING))
    intercept[IllegalArgumentException] {
      IsoDate.validate(Schema.create(Schema.Type.INT))
    }
  }

}

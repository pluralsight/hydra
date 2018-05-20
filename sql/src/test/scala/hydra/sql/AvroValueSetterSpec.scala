package hydra.sql

import java.math.{MathContext, RoundingMode}
import java.nio.ByteBuffer
import java.sql._
import java.time.{LocalDate, ZoneId}

import com.google.common.collect.Lists
import com.pluralsight.hydra.sql.MockArray
import hydra.avro.convert.{ISODateConverter, IsoDate}
import hydra.avro.util.SchemaWrapper
import org.apache.avro.LogicalTypes.LogicalTypeFactory
import org.apache.avro.{LogicalType, LogicalTypes, Schema}
import org.apache.avro.generic.GenericData
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FunSpecLike, Matchers}


/**
  * Created by alexsilva on 5/4/17.
  */
class ValueSetterSpec extends Matchers with FunSpecLike with MockFactory {

  LogicalTypes.register(IsoDate.IsoDateLogicalTypeName, new LogicalTypeFactory {
    override def fromSchema(schema: Schema): LogicalType = IsoDate
  })

  val schemaStr =
    """
      |{
      |	"type": "record",
      |	"name": "User",
      |	"namespace": "hydra",
      |	"fields": [{
      |			"name": "id",
      |			"type": "int"
      |		},
      |		{
      |			"name": "username",
      |			"type": "string"
      |		},
      |		{
      |			"name": "rate",
      |   "type": {
      |			"type": "bytes",
      |			"logicalType": "decimal",
      |			"precision": 4,
      |			"scale": 2
      |   }
      |		},
      |		{
      |			"name": "active",
      |			"type": "boolean",
      |      "doc": "active_doc"
      |		},
      |		{
      |			"name": "score",
      |			"type": "float"
      |		},
      |		{
      |			"name": "scored",
      |			"type": "double"
      |		},
      |		{
      |			"name": "signupTimestamp",
      |			"type": {
      |				"type": "long",
      |				"logicalType": "timestamp-millis"
      |			}
      |		},
      |		{
      |			"name": "signupDate",
      |			"type": {
      |				"type": "int",
      |				"logicalType": "date"
      |			}
      |		},
      |		{
      |			"name": "testUnion",
      |			"type": ["null", "string"]
      |		},
      |  		{
      |			"name": "testNullUnion",
      |			"type": ["null", "string"]
      |		},
      |		{
      |			"name": "friends",
      |			"type": {
      |				"type": "array",
      |				"items": "string"
      |			}
      |		},
      |  {
      |    "name": "testEnum",
      |    "type": {
      |        "type": "enum",
      |        "name": "enum_type",
      |        "symbols": ["test1", "test2"]
      |    }
      |  },
      |      {"name": "address", "type":
      |      {"type": "record",
      |       "name": "AddressRecord",
      |       "fields": [
      |         {"name": "street", "type": "string"}
      |       ]}
      |    },
      |    {
      |			"name": "bigNumber",
      |			"type": "long"
      |		},
      |  {
      |			"name": "byteField",
      |			"type": "bytes"
      |		},
      |  {
      |			"name": "isoDate",
      |			"type": {
      |				"type": "string",
      |				"logicalType": "iso-datetime"
      |			}
      |		},
      |  {
      |      "name": "authors",
      |      "type": {
      |        "type": "array",
      |        "items": {
      |          "type": "record",
      |          "name": "authors_record",
      |          "fields": [
      |            {
      |              "name": "id",
      |              "type": "string"
      |            },
      |            {
      |              "name": "authorHandle",
      |              "type": "string"
      |            }
      |          ]
      |        }
      |      }
      |    }
      |	]
      |}
    """.stripMargin

  val schema = SchemaWrapper.from(new Schema.Parser().parse(schemaStr))

  val valueSetter = new AvroValueSetter(schema, PostgresDialect)
  describe("The AvroValueSetter") {
    it("sets values in inserts") {
      val ts = System.currentTimeMillis
      val ctx = new MathContext(4, RoundingMode.HALF_EVEN)
      val decimal = new java.math.BigDecimal("0.2", ctx).setScale(2)
      val dt = LocalDate.ofEpochDay(1234).atStartOfDay(ZoneId.systemDefault()).toInstant.toEpochMilli
      val isoDate = new ISODateConverter().fromCharSequence("2015-07-28T19:55:57.693217+00:00",
        Schema.create(Schema.Type.STRING), IsoDate).toInstant.toEpochMilli

      val mockedStmt = mock[PreparedStatement]

      val connection = mock[Connection]
      val friends = Lists.newArrayList("friend1", "friend2")
      (mockedStmt.getConnection _).expects().returning(connection)
      val mockArray = new MockArray(friends)
      (connection.createArrayOf(_, _)).expects("VARCHAR", *).returning(mockArray)
      (mockedStmt.setInt _).expects(1, 1)
      (mockedStmt.setString _).expects(2, "alex")
      (mockedStmt.setBigDecimal _).expects(3, decimal)
      (mockedStmt.setBoolean _).expects(4, true)
      (mockedStmt.setFloat _).expects(5, 10f)
      (mockedStmt.setDouble _).expects(6, 2.5d)
      (mockedStmt.setTimestamp(_: Int, _: Timestamp)).expects(7, new Timestamp(ts))
      (mockedStmt.setDate(_: Int, _: Date)).expects(8, new Date(dt))
      (mockedStmt.setString _).expects(9, "test")
      (mockedStmt.setNull(_: Int, _: Int)).expects(10, java.sql.Types.VARCHAR)
      (mockedStmt.setArray _).expects(11, mockArray)
      (mockedStmt.setString _).expects(12, "test1")
      (mockedStmt.setString _).expects(13, """{"street": "happy drive"}""")
      (mockedStmt.setLong _).expects(14, 12342134223L)
      (mockedStmt.setBytes _).expects(15, *) //todo: how to verify the contents of an array in scala mock?
      (mockedStmt.setTimestamp(_: Int, _: Timestamp)).expects(16, new Timestamp(isoDate))
      (mockedStmt.setString _).expects(17, """[{"id": "authorId", "authorHandle": "theHandle"}]""")
      (mockedStmt.addBatch _).expects()

      val avroSchema = schema.schema
      val record = new GenericData.Record(avroSchema)
      record.put("id", 1)
      record.put("username", "alex")
      record.put("rate", 0.2d)
      record.put("active", true)
      record.put("score", 10f)
      record.put("scored", 2.5d)
      record.put("signupTimestamp", ts)
      record.put("signupDate", 1234)
      record.put("friends", new GenericData.Array[String](avroSchema.getField("friends").schema(), friends))
      record.put("testUnion", "test")
      record.put("testNullUnion", null)
      record.put("testEnum", "test1")
      val address = new GenericData.Record(avroSchema.getField("address").schema)
      address.put("street", "happy drive")
      record.put("address", address)
      record.put("bigNumber", 12342134223L)
      record.put("byteField", ByteBuffer.wrap("test".getBytes))
      record.put("isoDate", "2015-07-28T19:55:57.693217+00:00")
      val authors = new GenericData.Array[GenericData.Record](1, avroSchema.getField("authors").schema)
      val eleSch = new Schema.Parser().parse(
        """
          | {"type": "record",
          |          "name": "authors_record",
          |          "fields": [
          |            {
          |              "name": "id",
          |              "type": "string"
          |            },
          |            {
          |              "name": "authorHandle",
          |              "type": "string"
          |            }]}
        """.stripMargin)
      val author = new GenericData.Record(eleSch)
      author.put("id", "authorId")
      author.put("authorHandle", "theHandle")
      authors.add(author)
      record.put("authors", authors)
      valueSetter.bind(record, mockedStmt)
    }

    it("gets insert fields from the dialect") {
      val binder = new AvroValueSetter(schema, PostgresDialect)
      binder.fieldTypes shouldBe schema.getFields
        .map(f => f -> JdbcUtils.getJdbcType(f.schema(), PostgresDialect)).toMap
    }

    it("gets upsert fields from the dialect") {
      val schemaStr =
        """
          |{
          |	"type": "record",
          |	"name": "FlushTest",
          |	"namespace": "hydra",
          | "hydra.key":"id",
          |	"fields": [{
          |			"name": "id",
          |			"type": "int",
          |			"doc": "doc"
          |		},
          |		{
          |			"name": "username",
          |			"type": ["null", "string"]
          |		}
          |	]
          |}""".stripMargin

      val sch = SchemaWrapper.from(new Schema.Parser().parse(schemaStr))
      val binder = new AvroValueSetter(sch, PostgresDialect)
      binder.fieldTypes shouldBe PostgresDialect.upsertFields(sch)
        .map(f => f -> JdbcUtils.getJdbcType(f.schema(), PostgresDialect)).toMap
    }
  }
}

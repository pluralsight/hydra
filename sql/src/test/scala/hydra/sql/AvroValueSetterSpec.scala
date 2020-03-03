package hydra.sql

import java.math.{MathContext, RoundingMode}
import java.nio.ByteBuffer
import java.sql._
import java.time.{LocalDate, ZoneId}
import java.util.UUID

import com.google.common.collect.Lists
import com.pluralsight.hydra.avro.JsonConverter
import com.pluralsight.hydra.sql.MockArray
import hydra.avro.convert.{AvroUuid, ISODateConverter, IsoDate}
import hydra.avro.util.SchemaWrapper
import org.apache.avro.Conversions.UUIDConversion
import org.apache.avro.LogicalTypes.LogicalTypeFactory
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.{LogicalType, LogicalTypes, Schema}
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FunSpecLike, Matchers}

import scala.collection.JavaConverters._

/**
  * Created by alexsilva on 5/4/17.
  */
class AvroValueSetterSpec extends Matchers with FunSpecLike with MockFactory {

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
      |			"name": "uuid",
      |			"type": {
      |				"type": "string",
      |				"logicalType": "uuid"
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
      val dt = LocalDate
        .ofEpochDay(1234)
        .atStartOfDay(ZoneId.systemDefault())
        .toInstant
        .toEpochMilli
      val isoDate = new ISODateConverter()
        .fromCharSequence(
          "2015-07-28T19:55:57.693217+00:00",
          Schema.create(Schema.Type.STRING),
          IsoDate
        )
        .toInstant
        .toEpochMilli

      val uuid = UUID.randomUUID()
      val uuidV = new UUIDConversion().fromCharSequence(
        uuid.toString,
        Schema.create(Schema.Type.STRING),
        AvroUuid
      )

      val mockedStmt = mock[PreparedStatement]
      val connection = mock[Connection]
      val friends = Lists.newArrayList("friend1", "friend2")
      (mockedStmt.getConnection _).expects().returning(connection)
      val mockArray = new MockArray(friends)
      (connection
        .createArrayOf(_, _))
        .expects("VARCHAR", *)
        .returning(mockArray)
      (mockedStmt.setInt _).expects(1, 1)
      (mockedStmt.setString _).expects(2, "alex")
      (mockedStmt.setBigDecimal _).expects(3, decimal)
      (mockedStmt.setBoolean _).expects(4, true)
      (mockedStmt.setFloat _).expects(5, 10f)
      (mockedStmt.setDouble _).expects(6, 2.5d)
      (mockedStmt
        .setTimestamp(_: Int, _: Timestamp))
        .expects(7, new Timestamp(ts))
      (mockedStmt.setDate(_: Int, _: Date)).expects(8, new Date(dt))
      (mockedStmt.setString _).expects(9, "test")
      (mockedStmt.setNull(_: Int, _: Int)).expects(10, java.sql.Types.VARCHAR)
      (mockedStmt.setArray _).expects(11, mockArray)
      (mockedStmt.setString _).expects(12, "test1")
      (mockedStmt.setString _).expects(13, """{"street": "happy drive"}""")
      (mockedStmt.setLong _).expects(14, 12342134223L)
      (mockedStmt.setBytes _).expects(
        15,
        *
      ) //todo: how to verify the contents of an array in scala mock?
      (mockedStmt
        .setTimestamp(_: Int, _: Timestamp))
        .expects(16, new Timestamp(isoDate))
      (mockedStmt.setObject(_: Int, _: Any)).expects(17, *)
      (mockedStmt.setString _)
        .expects(18, """[{"id": "authorId", "authorHandle": "theHandle"}]""")
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
      record.put(
        "friends",
        new GenericData.Array[String](
          avroSchema.getField("friends").schema(),
          friends
        )
      )
      record.put("testUnion", "test")
      record.put("testNullUnion", null)
      record.put("testEnum", "test1")
      val address =
        new GenericData.Record(avroSchema.getField("address").schema)
      address.put("street", "happy drive")
      record.put("address", address)
      record.put("bigNumber", 12342134223L)
      record.put("byteField", ByteBuffer.wrap("test".getBytes))
      record.put("isoDate", "2015-07-28T19:55:57.693217+00:00")
      val authors = new GenericData.Array[GenericData.Record](
        1,
        avroSchema.getField("authors").schema
      )
      val eleSch =
        new Schema.Parser().parse("""
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
      record.put("uuid", uuid.toString)
      valueSetter.bind(record, mockedStmt)
    }

    it("gets insert fields from the dialect") {
      val binder = new AvroValueSetter(schema, PostgresDialect)
      binder.fieldTypes shouldBe schema.getFields
        .map(f => f -> JdbcUtils.getJdbcType(f.schema(), PostgresDialect))
        .toMap
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
      binder.fieldTypes shouldBe PostgresDialect
        .upsertFields(sch)
        .map(f => f -> JdbcUtils.getJdbcType(f.schema(), PostgresDialect))
        .toMap
    }

    it("works with json arrays") {
      val json =
        """
          | {
          | 	"id": "1c762732-94f6-4678-a328-1f996d127c3f",
          | 	"assessment": null,
          | 	"contentPillar": "it-ops",
          | 	"description": "desc.",
          | 	"highlights": "highlists",
          | 	"numberOfCourses": 2,
          | 	"numberOfHours": 11,
          | 	"prerequisites": "experience",
          | 	"retired": false,
          | 	"replacedById": null,
          | 	"status": "published",
          | 	"title": "Project+",
          | 	"thumbnailUrl": "url",
          | 	"type": "certificate",
          | 	"url": "/paths/certificate/comptia-project-plus",
          | 	"urlSlug": "comptia-project-plus",
          | 	"version": 1,
          | 	"createdAt": "2016-05-13T14:48:19.998Z",
          | 	"updatedAt": "2016-10-19T18:34:20.998Z",
          | 	"publishedAt": "2016-05-13T14:48:19.998Z",
          | 	"authors": [{
          | 		"id": "id",
          | 		"authorHandle": "handle"
          | 	}],
          | 	"pathLevels": [{
          | 		"id": "adf41442-6ec7-48ca-ad7f-2f8bfe57346d",
          | 		"transcenderExamId": null,
          | 		"title": "courses",
          | 		"description": "desc.",
          | 		"courses": [{
          | 			"id": "course1",
          | 			"deprecatedCourseId": "pt1"
          | 		}, {
          | 			"id": "course2",
          | 			"deprecatedCourseId": "pt2"
          | 		}],
          | 		"comingSoonCourses": []
          | 	}],
          | 	"relatedTopics": [{
          | 		"title": "PRINCE2"
          | 	}, {
          | 		"title": "PMP"
          | 	}]
          | }
        """.stripMargin

      val schema =
        """
          |{ "type": "record", "name": "ComplexTest", "namespace": "hydra.json", "fields": [{ "name": "id", "type": "string", "doc": "GUID Identifier" }, { "name": "assessment", "type": ["null", { "type": "record", "name": "assessment_record", "fields": [{ "name": "id", "type": "string" }] }], "default": null }, { "name": "contentPillar", "type": "string" }, { "name": "description", "type": "string" }, { "name": "highlights", "type": "string" }, { "name": "numberOfCourses", "type": "int" }, { "name": "numberOfHours", "type": "int" }, { "name": "prerequisites", "type": "string" }, { "name": "retired", "type": "boolean" }, { "name": "replacedById", "type": ["null", "string"], "default": null }, { "name": "status", "type": "string" }, { "name": "title", "type": "string", "doc": "Title for the Path" }, { "name": "thumbnailUrl", "type": "string" }, { "name": "type", "type": "string" }, { "name": "url", "type": "string" }, { "name": "urlSlug", "type": "string" }, { "name": "version", "type": "int" }, { "name": "createdAt", "type": { "type": "string", "logicalType": "iso-datetime" } }, { "name": "updatedAt", "type": { "type": "string", "logicalType": "iso-datetime" } }, { "name": "publishedAt", "type": { "type": "string", "logicalType": "iso-datetime" } }, { "name": "authors", "type": { "type": "array", "items": { "type": "record", "name": "authors_Record", "fields": [{ "name": "id", "type": "string" }, { "name": "authorHandle", "type": "string" }] } } }, { "name": "pathLevels", "type": { "type": "array", "items": { "type": "record", "name": "pathLevels_Record", "fields": [{ "name": "id", "type": "string" }, { "name": "transcenderExamId", "type": ["null", "string"], "default": null }, { "name": "title", "type": "string" }, { "name": "description", "type": "string" }, { "name": "courses", "type": { "type": "array", "items": { "type": "record", "name": "courses_Record", "fields": [{ "name": "id", "type": "string" }, { "name": "deprecatedCourseId", "type": "string" }] } } }, { "name": "comingSoonCourses", "type": ["null", { "type": "array", "items": { "type": "record", "name": "comingSoonCoursesRecord", "fields": [{ "name": "id", "type": "string" }] } }], "default": null }] } } }, { "name": "relatedTopics", "type": { "type": "array", "items": { "type": "record", "name": "relatedTopicsRecord", "fields": [{ "name": "title", "type": "string" }] } } }] }{ "type": "record", "name": "ComplexTest", "namespace": "hydra.json", "fields": [{ "name": "id", "type": "string", "doc": "GUID Identifier" }, { "name": "assessment", "type": ["null", { "type": "record", "name": "assessment_record", "fields": [{ "name": "id", "type": "string" }] }], "default": null }, { "name": "contentPillar", "type": "string" }, { "name": "description", "type": "string" }, { "name": "highlights", "type": "string" }, { "name": "numberOfCourses", "type": "int" }, { "name": "numberOfHours", "type": "int", "default": null }, { "name": "prerequisites", "type": "string" }, { "name": "retired", "type": "boolean" }, { "name": "replacedById", "type": ["null", "string"], "default": null }, { "name": "status", "type": "string" }, { "name": "title", "type": "string", "doc": "Title for the Path" }, { "name": "thumbnailUrl", "type": "string" }, { "name": "type", "type": "string" }, { "name": "url", "type": "string" }, { "name": "urlSlug", "type": "string" }, { "name": "version", "type": "int" }, { "name": "createdAt", "type": { "type": "string", "logicalType": "iso-datetime" } }, { "name": "updatedAt", "type": { "type": "string", "logicalType": "iso-datetime" } }, { "name": "publishedAt", "type": { "type": "string", "logicalType": "iso-datetime" } }, { "name": "authors", "type": { "type": "array", "items": { "type": "record", "name": "authors_Record", "fields": [{ "name": "id", "type": "string" }, { "name": "authorHandle", "type": "string" }] } } }, { "name": "pathLevels", "type": { "type": "array", "items": { "type": "record", "name": "pathLevels_Record", "fields": [{ "name": "id", "type": "string" }, { "name": "transcenderExamId", "type": ["null", "string"], "default": null }, { "name": "title", "type": "string" }, { "name": "description", "type": "string" }, { "name": "courses", "type": { "type": "array", "items": { "type": "record", "name": "courses_Record", "fields": [{ "name": "id", "type": "string" }, { "name": "deprecatedCourseId", "type": "string" }] } } }, { "name": "comingSoonCourses", "type": ["null", { "type": "array", "items": { "type": "record", "name": "comingSoonCoursesRecord", "fields": [{ "name": "id", "type": "string" }] } }], "default": null }] } } }, { "name": "relatedTopics", "type": { "type": "array", "items": { "type": "record", "name": "relatedTopicsRecord", "fields": [{ "name": "title", "type": "string" }] } } }] }
          |""".stripMargin

      val record = new JsonConverter[GenericRecord](
        new Schema.Parser().parse(schema)
      ).convert(json)
      //in avro complex types are Lists
      record.get("relatedTopics") shouldBe a[java.util.List[_]]
      val s = new AvroValueSetter(
        SchemaWrapper.from(record.getSchema),
        PostgresDialect
      )
      val mockedStmt = mock[PreparedStatement]
      val expected = """[{"title": "PRINCE2"},{"title": "PMP"}]"""
      (mockedStmt.setString _).expects(1, expected)

      s.arrayValue(
        record
          .get("relatedTopics")
          .asInstanceOf[java.util.List[_]]
          .asScala
          .toList,
        record.getSchema().getField("relatedTopics").schema(),
        mockedStmt,
        1
      )

    }

    it("Removes null bytes from strings when the dialect is Postgres") {
      val simpleSchemaString =
        """
          |{
          |     "type": "record",
          |     "namespace": "hydra",
          |     "name": "FullName",
          |     "fields": [
          |       { "name": "first", "type": "string" },
          |       { "name": "last", "type": "string" }
          |     ]
          |}
        """.stripMargin
      val schemaWrapper =
        SchemaWrapper.from(new Schema.Parser().parse(simpleSchemaString))

      val testRecord = {
        val tr = new GenericData.Record(schemaWrapper.schema)
        tr.put("first", "FirstName")
        tr.put("last", "Last\u0000Name\u0000")
        tr
      }

      val mockedStatement = {
        val ps = mock[PreparedStatement]
        (ps.setString _).expects(1, "FirstName")
        (ps.setString _).expects(2, "LastName")
        (ps.addBatch _).expects()
        ps
      }

      val avroValueSetter = new AvroValueSetter(schemaWrapper, PostgresDialect)
      avroValueSetter.bind(testRecord, mockedStatement)
    }

    it("binds correctly for deletion") {
      val simpleSchemaString =
        """
          |{
          |     "type": "record",
          |     "namespace": "hydra",
          |     "name": "RandomRecord",
          |     "fields": [
          |       { "name": "id", "type": "int" },
          |       { "name": "value", "type": "string" }
          |     ]
          |}
        """.stripMargin
      val schema = new Schema.Parser().parse(simpleSchemaString)
      val schemaWrapper = SchemaWrapper.from(schema)
      val avroValueSetter = new AvroValueSetter(schemaWrapper, PostgresDialect)

      val preparedStatement = {
        val ps = mock[PreparedStatement]
        (ps.setInt _).expects(1, 100)
        (ps.addBatch _).expects()
        ps
      }

      avroValueSetter.bindForDeletion(
        Map(schema.getField("id") -> 100.asInstanceOf[AnyRef]),
        preparedStatement
      )
    }
  }
}

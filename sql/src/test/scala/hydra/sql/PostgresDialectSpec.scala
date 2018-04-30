package hydra.sql

import java.sql.JDBCType
import java.sql.JDBCType._

import hydra.avro.convert.IsoDate
import hydra.avro.util.SchemaWrapper
import org.apache.avro.LogicalTypes.LogicalTypeFactory
import org.apache.avro.{LogicalType, LogicalTypes, Schema}
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

/**
  * Created by alexsilva on 5/4/17.
  */
class PostgresDialectSpec extends Matchers
  with FunSpecLike
  with BeforeAndAfterAll {

  LogicalTypes.register(IsoDate.IsoDateLogicalTypeName,new LogicalTypeFactory {
    override def fromSchema(schema: Schema): LogicalType = IsoDate
  })

  implicit def fromSchema(schema: Schema): SchemaWrapper = SchemaWrapper.from(schema)

  val schema =
    """
      |{
      |	"type": "record",
      |	"name": "User",
      |	"namespace": "hydra",
      |	"fields": [{
      |			"name": "id",
      |			"type": "int",
      |     "meta":"primary-key"
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
      |			"name": "rateb",
      |			"type": "bytes"
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
      |			"name": "passwordHash",
      |			"type": "bytes"
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
      |  {
      |			"name": "testTS",
      |			"type": {
      |				"type": "string",
      |				"logicalType": "iso-datetime"
      |			}
      |		},
      |		{
      |			"name": "friends",
      |			"type": {
      |				"type": "array",
      |				"items": "string"
      |			}
      |		}
      |	]
      |}
    """.stripMargin


  describe("The postgres dialect") {
    it("has the right truncate statement") {
      PostgresDialect.isCascadingTruncateTable() shouldBe Some(true)
      PostgresDialect.getTruncateQuery("table") shouldBe """TRUNCATE TABLE table"""
    }

    it("converts a schema") {
      val avro = new Schema.Parser().parse(schema)
      PostgresDialect.getJDBCType(avro.getField("username").schema()).get shouldBe JdbcType("TEXT", CHAR)
      PostgresDialect.getJDBCType(avro.getField("passwordHash").schema()).get shouldBe JdbcType("BYTEA", BINARY)
      PostgresDialect.getJDBCType(avro.getField("rate").schema()) shouldBe Some(JdbcType("DECIMAL(4,2)", DECIMAL))
      PostgresDialect.getJDBCType(avro.getField("active").schema()) shouldBe Some(JdbcType("BOOLEAN", BOOLEAN))
      PostgresDialect.getJDBCType(avro.getField("score").schema()) shouldBe Some(JdbcType("FLOAT4", FLOAT))
      PostgresDialect.getJDBCType(avro.getField("scored").schema()) shouldBe Some(JdbcType("FLOAT8", DOUBLE))
      PostgresDialect.getJDBCType(avro.getField("testUnion").schema()) shouldBe Some(JdbcType("TEXT", CHAR))
      PostgresDialect.getJDBCType(avro.getField("friends").schema()) shouldBe Some(JdbcType("TEXT[]", ARRAY))
      PostgresDialect.getJDBCType(avro.getField("signupDate").schema()) shouldBe None
      PostgresDialect.getJDBCType(avro.getField("testTS").schema()).get shouldBe JdbcType("TIMESTAMP", TIMESTAMP)
    }

    it("works with record types") {
      val schema =
        """
          | {"namespace": "hydra.avro.utils",
          |  "type": "record",
          |  "name": "Customer",
          |  "fields": [
          |    {"name": "name", "type": "string"},
          |    {"name": "address", "type":
          |      {"type": "record",
          |       "name": "AddressRecord",
          |       "fields": [
          |         {"name": "streetAddress", "type": "string"},
          |         {"name": "city", "type": "string"},
          |         {"name": "state", "type": "string"},
          |         {"name": "zip", "type": "string"}
          |       ]}
          |    }
          |  ]
          |}
        """.stripMargin

      val avro = new Schema.Parser().parse(schema)
      PostgresDialect.getJDBCType(avro.getField("address").schema())
        .get shouldBe JdbcType("JSON", JDBCType.CHAR)
    }

    it("returns the right placeholder for json") {
      PostgresDialect.jsonPlaceholder shouldBe "to_json(?::TEXT)"
    }

    it("works with general sql commands") {
      PostgresDialect.getTableExistsQuery("table") shouldBe "SELECT 1 FROM table LIMIT 1"

      PostgresDialect.getSchemaQuery("table") shouldBe "SELECT * FROM table WHERE 1=0"
    }

    it("uses a json column") {
      val schema =
        """
          |{
          |	"type": "record",
          |	"name": "User",
          |	"namespace": "hydra",
          | "hydra.key": "id",
          |	"fields": [{
          |			"name": "id",
          |			"type": "int",
          |			"doc": "doc"
          |		},
          |		{
          |			"name": "username",
          |			"type": ["null", "string"]
          |		},
          |      {"name": "address", "type":
          |      {"type": "record",
          |       "name": "AddressRecord",
          |       "fields": [
          |         {"name": "streetAddress", "type": "string"}
          |       ]}
          |    }
          |	]
          |}""".stripMargin

      val avro = new Schema.Parser().parse(schema)

      PostgresDialect.insertStatement("table", avro,
        UnderscoreSyntax) shouldBe "INSERT INTO table (\"id\",\"username\",\"address\") VALUES (?,?,to_json(?::TEXT))"
    }

    it("builds an upsert") {

      val schema =
        """
          |{
          |	"type": "record",
          |	"name": "User",
          |	"namespace": "hydra",
          | "hydra.key": "id",
          |	"fields": [{
          |			"name": "id",
          |			"type": "int",
          |			"doc": "doc"
          |		},
          |		{
          |			"name": "username",
          |			"type": ["null", "string"]
          |		},
          |      {"name": "address", "type":
          |      {"type": "record",
          |       "name": "AddressRecord",
          |       "fields": [
          |         {"name": "streetAddress", "type": "string"}
          |       ]}
          |    }
          |	]
          |}""".stripMargin

      val avro = new Schema.Parser().parse(schema)

      val stmt = PostgresDialect.buildUpsert("table", avro, UnderscoreSyntax)

      val expected =
        """insert into table ("id","username","address") values (?,?,to_json(?::TEXT))
          |on conflict ("id")
          |do update set ("username","address") = (?,to_json(?::TEXT))
          |where table."id"=?;""".stripMargin

      stmt shouldBe expected
    }

    it("builds an upsert with composite primary keys") {

      val schema =
        """
          |{
          |	"type": "record",
          |	"name": "User",
          |	"namespace": "hydra",
          | "hydra.key":"id1,id2",
          |	"fields": [{
          |			"name": "id1",
          |			"type": "int",
          |			"doc": "doc"
          |		},
          |  {
          |			"name": "id2",
          |			"type": "int",
          |			"doc": "doc"
          |		},
          |		{
          |			"name": "username",
          |			"type": ["null", "string"]
          |		}
          |	]
          |}""".stripMargin

      val avro = new Schema.Parser().parse(schema)

      val stmt = PostgresDialect.buildUpsert("table", avro, UnderscoreSyntax)

      val expected =
        """insert into table ("id1","id2","username") values (?,?,?)
          |on conflict ("id1","id2")
          |do update set ("username") = (?)
          |where table."id1"=? and table."id2"=?;""".stripMargin

      stmt shouldBe expected
    }

    it("returns the correct field list for inserts") {
      val schema =
        """
          |{
          |	"type": "record",
          |	"name": "User",
          |	"namespace": "hydra",
          |	"fields": [{
          |			"name": "id1",
          |			"type": "int",
          |			"doc": "doc"
          |		},
          |  {
          |			"name": "id2",
          |			"type": "int",
          |			"doc": "doc"
          |		},
          |		{
          |			"name": "username",
          |			"type": ["null", "string"]
          |		}
          |	]
          |}""".stripMargin

      val avro = new Schema.Parser().parse(schema)

      PostgresDialect.upsertFields(avro) shouldBe Seq(avro.getField("id1"), avro.getField("id2"),
        avro.getField("username"))
    }

    it("returns the correct field list for upserts") {
      val schema =
        """
          |{
          |	"type": "record",
          |	"name": "User",
          |	"namespace": "hydra",
          | "hydra.key":"id1,id2",
          |	"fields": [{
          |			"name": "id1",
          |			"type": "int",
          |			"doc": "doc"
          |		},
          |  {
          |			"name": "id2",
          |			"type": "int",
          |			"doc": "doc"
          |		},
          |		{
          |			"name": "username",
          |			"type": ["null", "string"]
          |		}
          |	]
          |}""".stripMargin

      val avro = new Schema.Parser().parse(schema)

      PostgresDialect.upsertFields(avro) shouldBe Seq(avro.getField("id1"), avro.getField("id2"),
        avro.getField("username"), avro.getField("username"), avro.getField("id1"), avro.getField("id2"))
    }

    it("Creates the correct alter table statements") {
      import scala.collection.JavaConverters._
      val schema = new Schema.Parser().parse(
        """
          |{
          |	"type": "record",
          |	"name": "User",
          |	"namespace": "hydra",
          | "key":"id1,id2",
          |	"fields": [{
          |			"name": "id1",
          |			"type": "int",
          |			"doc": "doc"
          |		},
          |  {
          |			"name": "id2",
          |			"type": "int",
          |			"doc": "doc"
          |		},
          |		{
          |			"name": "username",
          |			"type": ["null", "string"]
          |		}
          |	]
          |}""".stripMargin)

      val expected = Seq(
        """alter table test add column "id1" INTEGER""",
        """alter table test add column "id2" INTEGER""",
        """alter table test add column "username" TEXT""")

      PostgresDialect.alterTableQueries("test", schema.getFields().asScala, UnderscoreSyntax) shouldBe expected
    }
  }

  it("creates delete DML") {
    val schema = new Schema.Parser().parse(
      """
        |{
        |	"type": "record",
        |	"name": "User",
        |	"namespace": "hydra",
        | "key":"id1,id2",
        |	"fields": [{
        |			"name": "id1",
        |			"type": "int",
        |			"doc": "doc"
        |		},
        |  {
        |			"name": "id2",
        |			"type": "int",
        |			"doc": "doc"
        |		},
        |		{
        |			"name": "username",
        |			"type": ["null", "string"]
        |		}
        |	]
        |}""".stripMargin)

    intercept[AssertionError] {
      PostgresDialect.deleteStatement("test_table", Seq.empty, UnderscoreSyntax)
    }

    val singleKey = PostgresDialect.deleteStatement("test_table",
      Seq("id1"), UnderscoreSyntax)

    singleKey shouldBe """DELETE FROM test_table WHERE "id1" = ?"""

    val stmt = PostgresDialect.deleteStatement("test_table",
      Seq("id1", "id2"), UnderscoreSyntax)
    stmt shouldBe """DELETE FROM test_table WHERE "id1" = ? AND "id2" = ?"""
  }
}

package hydra.sql

import java.sql.JDBCType._

import hydra.avro.util.SchemaWrapper
import org.apache.avro.Schema
import org.scalatest.matchers.should.Matchers
import org.scalatest.funspec.AnyFunSpecLike

/**
  * Created by alexsilva on 5/4/17.
  */
class AggregatedDialectSpec extends Matchers with AnyFunSpecLike {

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

  val avro = new Schema.Parser().parse(schema)

  describe("The Aggregate dialect") {

    it("handles the right urls") {
      val dialect =
        new AggregatedDialect(List(PostgresDialect, new JdbcDialect() {
          override def canHandle(url: String): Boolean =
            url.startsWith("jdbc:postgresql")
        }))
      dialect.canHandle("jdbc:postgresql") shouldBe true
      dialect.canHandle("jdbc:db2") shouldBe false
    }

    it("builds upserts") {

      val schema =
        SchemaWrapper.from(new Schema.Parser().parse("""
          |{
          |	"type": "record",
          |	"name": "User",
          |	"namespace": "hydra",
          | "hydra.key": "id",
          |
          |	"fields": [{
          |			"name": "id",
          |			"type": "int"
          |		},
          |		{
          |			"name": "username",
          |			"type": "string"
          |		},
          |		{
          |			"name": "active",
          |			"type": "boolean"
          |		}
          |	]
          |}
        """.stripMargin))

      val dialect =
        new AggregatedDialect(List(PostgresDialect, new JdbcDialect() {
          override def canHandle(url: String): Boolean =
            url.startsWith("jdbc:postgresql")
        }))

      val upsert =
        """insert into table ("id","username","active") values (?,?,?)
          |on conflict ("id")
          |do update set "username" = EXCLUDED."username","active" = EXCLUDED."active";""".stripMargin
      dialect.buildUpsert("table", schema, UnderscoreSyntax) shouldBe upsert
    }

    it("converts a schema") {
      val dialect = new AggregatedDialect(List(PostgresDialect, DB2Dialect))
      dialect
        .getJDBCType(avro.getField("username").schema())
        .get shouldBe JdbcType("TEXT", VARCHAR)

      dialect
        .getJDBCType(avro.getField("passwordHash").schema())
        .get shouldBe JdbcType("BYTEA", BINARY)

      val dialect1 = new AggregatedDialect(List(DB2Dialect, PostgresDialect))
      dialect1
        .getJDBCType(avro.getField("username").schema())
        .get shouldBe JdbcType("CLOB", CLOB)
      dialect1.getJDBCType(avro.getField("rate").schema()) shouldBe Some(
        JdbcType("DECIMAL(4,2)", DECIMAL)
      )
    }
  }
}

package hydra.sql

import java.sql.JDBCType._

import org.apache.avro.Schema
import org.scalatest.matchers.should.Matchers
import org.scalatest.funspec.AnyFunSpecLike

/**
  * Created by alexsilva on 5/4/17.
  */
class DB2DialectSpec extends Matchers with AnyFunSpecLike {

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

  describe("The DB2 dialect") {
    it("converts a schema") {
      val avro = new Schema.Parser().parse(schema)
      DB2Dialect
        .getJDBCType(avro.getField("username").schema())
        .get shouldBe JdbcType("CLOB", CLOB)
      DB2Dialect.getJDBCType(avro.getField("passwordHash").schema()) shouldBe None
      DB2Dialect.getJDBCType(avro.getField("rate").schema()) shouldBe None
      DB2Dialect.getJDBCType(avro.getField("active").schema()) shouldBe Some(
        JdbcType("CHAR(1)", CHAR)
      )
      DB2Dialect.getJDBCType(avro.getField("score").schema()) shouldBe None
      DB2Dialect.getJDBCType(avro.getField("scored").schema()) shouldBe None
      DB2Dialect.getJDBCType(avro.getField("testUnion").schema()) shouldBe None
      DB2Dialect.getJDBCType(avro.getField("friends").schema()) shouldBe None
      DB2Dialect.getJDBCType(avro.getField("signupDate").schema()) shouldBe None
    }

    it("works with general sql commands") {
      DB2Dialect.getTableExistsQuery("table") shouldBe "SELECT * FROM table WHERE 1=0"

      DB2Dialect.getSchemaQuery("table") shouldBe "SELECT * FROM table WHERE 1=0"
    }
  }
}

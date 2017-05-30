package hydra.jdbc.sql

import java.sql.Types._

import org.apache.avro.Schema
import org.scalatest.{FunSpecLike, Matchers}

/**
  * Created by alexsilva on 5/18/17.
  */
class JdbcUtilsSpec extends Matchers with FunSpecLike {

  val schema =
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
      |			"name": "rateb",
      |			"type": "bytes"
      |		},
      |		{
      |			"name": "active",
      |			"type": "boolean"
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

  describe("The postgres dialect") {
    it("converts a schema") {
      JdbcUtils.getCommonJDBCType(avro.getField("username").schema()).get shouldBe JdbcType("TEXT", VARCHAR)
      JdbcUtils.getCommonJDBCType(avro.getField("passwordHash").schema()).get shouldBe JdbcType("BYTE", TINYINT)
      JdbcUtils.getCommonJDBCType(avro.getField("rate").schema()) shouldBe Some(JdbcType("DECIMAL(4,2)", DECIMAL))
      JdbcUtils.getCommonJDBCType(avro.getField("active").schema()) shouldBe Some(JdbcType("BIT(1)", BIT))
      JdbcUtils.getCommonJDBCType(avro.getField("score").schema()) shouldBe Some(JdbcType("REAL", FLOAT))
      JdbcUtils.getCommonJDBCType(avro.getField("scored").schema()) shouldBe Some(JdbcType("DOUBLE PRECISION", DOUBLE))
      JdbcUtils.getCommonJDBCType(avro.getField("testUnion").schema()) shouldBe Some(JdbcType("TEXT", VARCHAR))
      JdbcUtils.getCommonJDBCType(avro.getField("friends").schema()) shouldBe None
      JdbcUtils.getCommonJDBCType(avro.getField("signupDate").schema()) shouldBe Some(JdbcType("DATE", DATE))
      JdbcUtils.getCommonJDBCType(avro.getField("signupTimestamp").schema()) shouldBe Some(JdbcType("TIMESTAMP", TIMESTAMP))
    }

    it("extracts the right column list") {
      JdbcUtils.columns(avro) shouldBe Seq("id", "username", "rate", "rateb", "active", "score", "scored",
        "passwordHash", "signupTimestamp", "signupDate", "testUnion", "friends")
    }
  }
}

package hydra.jdbc.sql

import java.sql.Types

import org.apache.avro.Schema
import org.scalatest.{FunSpecLike, Matchers}

/**
  * Created by alexsilva on 5/4/17.
  */
class PostgresDialectSpec extends Matchers with FunSpecLike {
  val schema =
    """
      |{
      |  "type": "record",
      |  "name": "User",
      |  "namespace": "hydra",
      |  "fields": [
      |    {"name": "id","type": "int"},
      |    {"name": "username","type": "string" },
      |    {"name": "passwordHash","type": "bytes"},
      |    {"name": "signupTimestamp", "type": {"type": "long", "logicalType": "timestamp-millis"}},
      |    {"name": "signupDate","type": {"type": "int", "logicalType": "date"}}
      |  ]
      | }
      |
    """.stripMargin

  describe("The postgres dialect") {
    it("converts a schema") {
      val avro = new Schema.Parser().parse(schema)
      PostgresDialect.getJDBCType(avro.getField("signupTimestamp").schema()).get shouldBe JdbcType("TIMESTAMP", java.sql.Types.TIMESTAMP)
      PostgresDialect.getJDBCType(avro.getField("id").schema()).get shouldBe JdbcType("INTEGER", Types.INTEGER)
      PostgresDialect.getJDBCType(avro.getField("username").schema()).get shouldBe JdbcType("TEXT", Types.CHAR)
      PostgresDialect.getJDBCType(avro.getField("passwordHash").schema()).get shouldBe JdbcType("BYTEA", Types.BINARY)
      PostgresDialect.getJDBCType(avro.getField("signupDate").schema()).get shouldBe JdbcType("DATE", Types.DATE)

    }
  }

}

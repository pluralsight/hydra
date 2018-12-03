package hydra.sql

import java.sql.JDBCType._
import java.sql.{DriverManager, JDBCType}

import hydra.avro.convert.AvroUuid
import hydra.avro.util.SchemaWrapper
import hydra.common.util.TryWith
import org.apache.avro.LogicalTypes.LogicalTypeFactory
import org.apache.avro.Schema.Type
import org.apache.avro.{LogicalType, LogicalTypes, Schema}
import org.h2.jdbc.JdbcSQLException
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

import scala.concurrent.duration._

/**
  * Created by alexsilva on 5/18/17.
  */
class JdbcUtilsSpec extends Matchers
  with FunSpecLike
  with BeforeAndAfterAll {

  DriverManager.registerDriver(new org.h2.Driver)

  val provider = new DriverManagerConnectionProvider("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1",
    "", "", 1, 1.millis)


  LogicalTypes.register(AvroUuid.AvroUuidLogicalTypeName, new LogicalTypeFactory {
    override def fromSchema(schema: Schema): LogicalType = AvroUuid
  })

  override def afterAll() = provider.connection.close()

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
      |  		{
      |			"name": "scoreLong",
      |			"type": "long"
      |		},
      |		{
      |			"name": "signupDate",
      |			"type": {
      |				"type": "int",
      |				"logicalType": "date"
      |			}
      |		},
      |  	{
      |			"name": "justANumber",
      |			"type": "int"
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

  describe("The JDBCUtils class") {
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
      JdbcUtils.getCommonJDBCType(avro.getField("scoreLong").schema()) shouldBe Some(JdbcType("BIGINT", BIGINT))
      JdbcUtils.getCommonJDBCType(avro.getField("justANumber").schema()) shouldBe Some(JdbcType("INTEGER", INTEGER))
    }

    it("extracts the right column list") {
      JdbcUtils.columnNames(avro) shouldBe Seq("id", "username", "rate", "rateb", "active", "score", "scored",
        "passwordHash", "signupTimestamp", "scoreLong", "signupDate", "justANumber", "testUnion", "friends")
    }

    it("gets non-nullable types for unions") {
      import scala.collection.JavaConverters._
      val u1 = Schema.createUnion(List(Schema.create(Type.INT), Schema.create(Type.NULL)).asJava)
      val u2 = Schema.createUnion(List(Schema.create(Type.NULL), Schema.create(Type.INT)).asJava)
      JdbcUtils.getNonNullableUnionType(u1) shouldBe Schema.create(Type.INT)
      JdbcUtils.getNonNullableUnionType(u2) shouldBe Schema.create(Type.INT)
    }

    it("errors with wrong unions") {
      val schema =
        """
          |{
          |	"type": "record",
          |	"name": "User",
          |	"namespace": "hydra",
          |	"fields": [
          |		{
          |			"name": "testUnion",
          |			"type": ["null", "string","int"]
          |		}
          |	]
          |}
        """.stripMargin

      val avro = new Schema.Parser().parse(schema)

      intercept[IllegalArgumentException] {
        JdbcUtils.getCommonJDBCType(avro.getField("testUnion").schema())
      }
    }

    it("creates a column sequence") {
      val schema =
        """
          |{
          |	"type": "record",
          |	"name": "User",
          |	"namespace": "hydra",
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

      val avro = new Schema.Parser().parse(schema)

      JdbcUtils.columns(avro, NoopDialect) shouldBe Seq(
        Column("id", avro.getField("id").schema(), JdbcType("INTEGER", JDBCType.INTEGER), false, Some("doc")),
        Column("username", avro.getField("username").schema(), JdbcType("TEXT", JDBCType.VARCHAR), true, None))
    }


    it("returns CHAR for enums") {
      val schema =
        """
          |{
          |	"type": "record",
          |	"name": "User",
          |	"namespace": "hydra",
          |	"fields": [
          |		{
          |			"name": "testEnum",
          |			"type": {
          |            "type": "enum",
          |            "name": "test_type",
          |            "symbols": ["test1", "test2"]
          |        }
          |		}
          |	]
          |}
        """.stripMargin

      val avro = new Schema.Parser().parse(schema)

      JdbcUtils.getCommonJDBCType(avro.getField("testEnum").schema()).get shouldBe JdbcType("TEXT", VARCHAR)

    }

    it("creates an avro schema") {

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
          |  		{
          |			"name": "scoreLong",
          |			"type": "long"
          |		},
          |		{
          |			"name": "signupDate",
          |			"type": {
          |				"type": "int",
          |				"logicalType": "date"
          |			}
          |		},
          |  	{
          |			"name": "justANumber",
          |			"type": "int"
          |		},
          |		{
          |			"name": "testUnion",
          |			"type": ["null", "string"]
          |		}
          |	]
          |}
        """.stripMargin

      val avro = new Schema.Parser().parse(schema)
      val columns = "\"id\" INTEGER NOT NULL,\"username\" TEXT NOT NULL,\"rate\" DECIMAL(4,2) NOT NULL,\"rateb\" BYTE" +
        " NOT NULL,\"active\" BIT(1) NOT NULL,\"score\" REAL NOT NULL,\"scored\" DOUBLE PRECISION NOT NULL," +
        "\"passwordHash\" BYTE NOT NULL,\"signupTimestamp\" TIMESTAMP NOT NULL,\"scoreLong\" BIGINT NOT NULL," +
        "\"signupDate\" DATE NOT NULL,\"justANumber\" INTEGER NOT NULL,\"testUnion\" TEXT "

      JdbcUtils.schemaString(SchemaWrapper.from(avro), "User", NoopDialect) shouldBe columns
    }

    it("Generates the correct ddl statement") {
      val schema =
        """
          |{
          |	"type": "record",
          |	"name": "User",
          |	"namespace": "hydra",
          | "hydra.key":"id",
          |	"fields": [
          | {
          |			"name": "id",
          |			"type": "int"
          |		},
          |		{
          |			"name": "username",
          |			"type": "string"
          |		},
          |  		{
          |			"name": "uuidTest",
          |			"type": {
          |				"type": "string",
          |				"logicalType": "uuid"
          |			}
          |		},
          |  {
          |			"name": "testEnum",
          |			"type": {
          |            "type": "enum",
          |            "name": "test_type",
          |            "symbols": ["test1", "test2"]
          |        }
          |		}
          |	]
          |}
        """.stripMargin

      val avro = new Schema.Parser().parse(schema)

      val stmt = JdbcUtils.schemaString(SchemaWrapper.from((avro)), "User", PostgresDialect)
      stmt shouldBe "\"id\" INTEGER NOT NULL,\"username\" TEXT NOT NULL,\"uuidTest\"" +
        " UUID NOT NULL,\"testEnum\" TEXT NOT NULL,CONSTRAINT \"User_PK\" PRIMARY KEY (\"id\")"
    }

    it("Generates the correct ddl statement with composite primary  keys") {
      val schema =
        """
          |{
          |	"type": "record",
          |	"name": "User",
          |	"namespace": "hydra",
          | "hydra.key":"id1,id2",
          |	"fields": [
          | {
          |			"name": "id1",
          |			"type": "int"
          |		},
          |  {
          |			"name": "id2",
          |			"type": "int"
          |		},
          |		{
          |			"name": "username",
          |			"type": "string"
          |		},
          |  {
          |			"name": "testEnum",
          |			"type": {
          |            "type": "enum",
          |            "name": "test_type",
          |            "symbols": ["test1", "test2"]
          |        }
          |		}
          |	]
          |}
        """.stripMargin

      val avro = new Schema.Parser().parse(schema)
      val stmt = JdbcUtils.schemaString(SchemaWrapper.from((avro)), "User", PostgresDialect)
      stmt shouldBe "\"id1\" INTEGER NOT NULL,\"id2\" INTEGER NOT NULL,\"username\" TEXT NOT NULL," +
        "\"testEnum\" TEXT NOT NULL,CONSTRAINT \"User_PK\" PRIMARY KEY (\"id1\",\"id2\")"
    }

    it("Generates the correct table name from a versioned schema") {
      val schema =
        """
          |{
          |	"type": "record",
          |	"name": "MyTable",
          |	"namespace": "hydra.v3",
          | "hydra.key":"id1,id2",
          |	"fields": [
          | {
          |			"name": "id1",
          |			"type": "int"
          |		}
          |	]
          |}
        """.stripMargin

      val avro = new Schema.Parser().parse(schema)
      val tableName = JdbcUtils.createTableNameFromSchema(avro)
      tableName shouldBe "MyTableV3"
    }

    it("Drops a table") {
      TryWith(provider.getConnection().createStatement()) { stmt =>
        val rs = stmt.executeUpdate("CREATE TABLE drop_test (\"id\" INTEGER NOT NULL,\"username\" TEXT ) ")
        rs shouldBe 0
      }.get
      JdbcUtils.dropTable(provider.getConnection(), "drop_test")
      intercept[JdbcSQLException] {
        TryWith(provider.getConnection().createStatement()) { stmt =>
          stmt.executeUpdate("""insert into drop_test values(1,'test')""") shouldBe 1
        }.get
      }
    }
  }
}

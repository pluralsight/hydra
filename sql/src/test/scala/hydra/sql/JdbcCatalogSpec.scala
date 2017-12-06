package hydra.sql

import java.sql.JDBCType
import java.util.Properties

import com.typesafe.config.ConfigFactory
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import org.apache.avro.{AvroRuntimeException, Schema}
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

/**
  * Created by alexsilva on 7/12/17.
  */
class JdbcCatalogSpec extends Matchers with FunSpecLike with BeforeAndAfterAll {

  import scala.collection.JavaConverters._

  val cfg = ConfigFactory.load().getConfig("db-cfg")

  val properties = new Properties

  cfg.entrySet().asScala.foreach(e => properties.setProperty(e.getKey(), cfg.getString(e.getKey())))

  private val hikariConfig = new HikariConfig(properties)

  private val ds = new HikariDataSource(hikariConfig)

  val store = new JdbcCatalog(ds, NoOpSyntax, H2Dialect)

  val schemaStr =
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

  val schema = new Schema.Parser().parse(schemaStr)

  override def beforeAll() = {
    store.createOrAlterTable(Table("test_table", schema))
    store.createSchema("test_schema") shouldBe true
    store.createOrAlterTable(Table("test_table", schema, dbSchema = Some("test_schema")))
  }

  override def afterAll() = {
    ds.close()
  }

  describe("The jdbc Catalog") {

    it("checks if a table exists") {
      store.tableExists(TableIdentifier("table")) shouldBe false
      store.tableExists(TableIdentifier("test_table")) shouldBe true
    }

    it("checks if a schema exists") {
      store.schemaExists("noschema") shouldBe false
      store.schemaExists("test_schema") shouldBe true
    }

    it("checks if a table with a schema exists") {
      store.tableExists(TableIdentifier("test_table", None, Some("test_schema"))) shouldBe true
      store.tableExists(TableIdentifier("table", None, Some("unknown"))) shouldBe false
    }

    it("errors if table exists") {
      intercept[UnableToCreateException] {
        store.createTable(Table("test_table", schema, Some("test_schema")))
      }
    }

    it("errors if it can't create a table in a different database") {
      intercept[UnableToCreateException] {
        store.createOrAlterTable(Table("test_table", schema, Some("x")))
      }
    }

    it("validates table names") {
      store.validateName("test")
      intercept[AnalysisException] {
        store.validateName("!not-valid")
      }
    }

    it("gets existing tables") {
      store.getTableMetadata(TableIdentifier("unknown")).isFailure shouldBe true
      store.getTableMetadata(TableIdentifier("unknown")).isFailure shouldBe true
      intercept[NoSuchSchemaException] {
        store.getTableMetadata(TableIdentifier("unknown", None, Some("unknown")))
      }
      val cols = List(DbColumn("id", JDBCType.INTEGER, false, Some("")),
        DbColumn("username", JDBCType.CLOB, true, Some("")))
      store.getTableMetadata(TableIdentifier("test_table", None, Some(""))).get shouldBe DbTable("test_table"
        , cols, None)
    }


    it("throws exception when trying to alter a table adding an optional field with no default value") {
      val newSchema = new Schema.Parser().parse(
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
          |		},
          |  {
          |			"name": "optional",
          |			"type": ["null", "string"]
          |		}
          |	]
          |}""".stripMargin)

      intercept[AvroRuntimeException] {
        store.createOrAlterTable(Table("test_table", newSchema))
      }
    }

    it("alters a table") {
      val newSchema = new Schema.Parser().parse(
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
          |		},
          |  {
          |			"name": "optional",
          |			"type": ["null", "string"],
          |     "default":"test"
          |		}
          |	]
          |}""".stripMargin)

      val dbTable = store.createOrAlterTable(Table("test_table", newSchema))

      val cols = List(
        DbColumn("id", JDBCType.INTEGER, false, Some("")),
        DbColumn("username", JDBCType.CLOB, true, Some("")),
        DbColumn("optional", JDBCType.CLOB, true, Some("")))
      store.getTableMetadata(TableIdentifier("test_table", None, Some(""))).get shouldBe DbTable("test_table", cols, None)
    }

    it("finds the missing fields for a schema") {
      val sc = new Schema.Parser().parse(
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
          |			"name": "firstName",
          |			"type": "string"
          |		},
          |  {
          |			"name": "lastName",
          |			"type": "string"
          |		}
          |	]
          |}""".stripMargin)

      val cols = List(
        DbColumn("id", JDBCType.INTEGER, false, Some("")),
        DbColumn("first_name", JDBCType.INTEGER, true, Some("")))

      val catalog = new JdbcCatalog(ds, UnderscoreSyntax, PostgresDialect)

      catalog.findMissingFields(sc, cols) shouldBe Seq(sc.getField("lastName"))
    }
  }
}

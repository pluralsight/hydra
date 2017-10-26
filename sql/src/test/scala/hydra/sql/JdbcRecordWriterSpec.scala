package hydra.sql

import java.util.Properties

import com.typesafe.config.ConfigFactory
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import hydra.avro.io.SaveMode
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

/**
  * Created by alexsilva on 5/4/17.
  */
class JdbcRecordWriterSpec extends Matchers with FunSpecLike with BeforeAndAfterAll with JdbcHelper {

  import scala.collection.JavaConverters._

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

  val cfg = ConfigFactory.load().getConfig("db-cfg")

  val properties = new Properties

  cfg.entrySet().asScala.foreach(e => properties.setProperty(e.getKey(), cfg.getString(e.getKey())))

  private val hikariConfig = new HikariConfig(properties)

  private val ds = new HikariDataSource(hikariConfig)

  val record = new GenericData.Record(schema)
  record.put("id", 1)
  record.put("username", "alex")

  val catalog = new JdbcCatalog(ds, UnderscoreSyntax, H2Dialect)

  override def afterAll() = ds.close()

  describe("The AvroWriter") {

    it("responds correctly it table already exists") {
      val schemaStr =
        """
          |{
          |	"type": "record",
          |	"name": "Tester",
          |	"namespace": "hydra",
          |	"fields": [{
          |			"name": "id",
          |			"type": "int"
          |		}
          |	]
          |}""".stripMargin

      catalog.createOrAlterTable(Table("tester", schema))
      val s = new Schema.Parser().parse(schemaStr)
      intercept[AnalysisException] {
        new JdbcRecordWriter(ds, s, SaveMode.ErrorIfExists, H2Dialect)
      }

      new JdbcRecordWriter(ds, s, SaveMode.Append, H2Dialect).close()
      new JdbcRecordWriter(ds, s, SaveMode.Overwrite, H2Dialect).close()
      new JdbcRecordWriter(ds, s, SaveMode.Ignore, H2Dialect).close()
    }

    it("creates a table") {
      val schemaStr =
        """
          |{
          |	"type": "record",
          |	"name": "CreateNew",
          |	"namespace": "hydra",
          |	"fields": [{
          |			"name": "id",
          |			"type": "int"
          |		}
          |	]
          |}""".stripMargin

      val s = new Schema.Parser().parse(schemaStr)
      new JdbcRecordWriter(ds, s, SaveMode.Append, H2Dialect).close()
      catalog.tableExists(TableIdentifier("tester")) shouldBe true
    }

    it("writes") {
      val writer = new JdbcRecordWriter(ds, schema, dialect = H2Dialect, batchSize = 1)
      writer.add(record)
      writer.flush()
      withConnection(ds.getConnection) { c =>
        val stmt = c.createStatement()
        val rs = stmt.executeQuery("select \"id\",\"username\" from user")
        rs.next()
        Seq(rs.getInt(1), rs.getString(2)) shouldBe Seq(1, "alex")
      }
      writer.close()
    }

    it("flushes") {

      val schemaStr =
        """
          |{
          |	"type": "record",
          |	"name": "FlushTest",
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


      val writer = new JdbcRecordWriter(ds, new Schema.Parser().parse(schemaStr), batchSize = 2, dialect = H2Dialect)
      writer.add(record)

      withConnection(ds.getConnection) { c =>
        val stmt = c.createStatement()
        val rs = stmt.executeQuery("select \"id\",\"username\" from flush_test")
        rs.next() shouldBe false
      }

      writer.flush()

      withConnection(ds.getConnection) { c =>
        val stmt = c.createStatement()
        val rs = stmt.executeQuery("select \"id\",\"username\" from flush_test")
        rs.next()
        Seq(rs.getInt(1), rs.getString(2)) shouldBe Seq(1, "alex")
      }

      writer.close()
    }

    it("flushesOnClose") {

      val schemaStr =
        """
          |{
          |	"type": "record",
          |	"name": "FlushOnClose",
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


      val writer = new JdbcRecordWriter(ds, new Schema.Parser().parse(schemaStr), batchSize = 2, dialect = H2Dialect)
      writer.add(record)

      withConnection(ds.getConnection) { c =>
        val stmt = c.createStatement()
        val rs = stmt.executeQuery("select \"id\",\"username\" from flush_on_close")
        rs.next() shouldBe false
      }

      writer.close()

      withConnection(ds.getConnection) { c =>
        val stmt = c.createStatement()
        val rs = stmt.executeQuery("select \"id\",\"username\" from flush_on_close")
        rs.next()
        Seq(rs.getInt(1), rs.getString(2)) shouldBe Seq(1, "alex")
      }
    }
  }
}

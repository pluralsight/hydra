package hydra.sql

import java.util.Properties

import com.typesafe.config.ConfigFactory
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import hydra.avro.io.SaveMode
import hydra.avro.util.SchemaWrapper
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecordBuilder}
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

/**
  * Created by alexsilva on 5/4/17.
  */
class JdbcRecordWriterSpec extends Matchers
  with FunSpecLike
  with BeforeAndAfterAll
  with JdbcHelper {

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

  val schema = SchemaWrapper.from(new Schema.Parser().parse(schemaStr))

  val cfg = ConfigFactory.load().getConfig("db-cfg")

  val properties = new Properties

  cfg.entrySet().asScala.foreach(e => properties.setProperty(e.getKey(), cfg.getString(e.getKey())))

  private val hikariConfig = new HikariConfig(properties)

  private val ds = new HikariDataSource(hikariConfig)

  val record = new GenericData.Record(schema.schema)
  record.put("id", 1)
  record.put("username", "alex")

  val catalog = new JdbcCatalog(ds, UnderscoreSyntax, H2Dialect)

  override def afterAll() = ds.close()

  describe("The JdbcRecordWriter") {

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
      val s = SchemaWrapper.from(new Schema.Parser().parse(schemaStr))
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
      new JdbcRecordWriter(ds, SchemaWrapper.from(s), SaveMode.Append, H2Dialect).close()
      catalog.tableExists(TableIdentifier("tester")) shouldBe true
    }

    it("writes") {
      val writer = new JdbcRecordWriter(ds, schema, dialect = H2Dialect, batchSize = 1)
      writer.addBatch(Upsert(record))
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


      val sch = new Schema.Parser().parse(schemaStr)
      val writer = new JdbcRecordWriter(ds, SchemaWrapper.from(sch),
        batchSize = 2, dialect = H2Dialect)

      val rec = new GenericRecordBuilder(sch)
      writer.addBatch(Upsert(rec.set("id", "1").set("username", "alex").build()))

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

    it("writes a single record") {
      val schemaStr =
        """
          |{
          |	"type": "record",
          |	"name": "WriteSingleRecord",
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

      val srecord = new GenericData.Record(new Schema.Parser().parse(schemaStr))
      srecord.put("id", 1)
      srecord.put("username", "alex")

      val writer = new JdbcRecordWriter(ds,
        SchemaWrapper.from(new Schema.Parser().parse(schemaStr)),
        batchSize = 2, dialect = H2Dialect)

      writer.execute(Upsert(srecord))

      withConnection(ds.getConnection) { c =>
        val stmt = c.createStatement()
        val rs = stmt.executeQuery("select \"id\",\"username\" from write_single_record")
        rs.next() shouldBe true
        rs.getString(2) shouldBe "alex"
        rs.getInt(1) shouldBe 1
      }

      //test schema update

      val newSchema =
        """
          |{
          |	"type": "record",
          |	"name": "WriteSingleRecord",
          |	"namespace": "hydra",
          |	"fields": [{
          |			"name": "id",
          |			"type": "int",
          |			"doc": "doc"
          |		},
          |  {
          |			"name": "rank",
          |			"type": "int"
          |		},
          |		{
          |			"name": "username",
          |			"type": ["null", "string"]
          |		}
          |	]
          |}""".stripMargin

      val newRecord = new GenericData.Record(new Schema.Parser().parse(newSchema))
      newRecord.put("id", 2)
      newRecord.put("rank", 2)
      newRecord.put("username", "alex-new")

      withConnection(ds.getConnection) { c =>
        val stmt = c.createStatement()
        stmt.executeUpdate("delete from write_single_record")
        stmt.close()
      }

      writer.execute(Upsert(newRecord))

      withConnection(ds.getConnection) { c =>
        val stmt = c.createStatement()
        val rs = stmt.executeQuery("select \"id\",\"username\", \"rank\" from write_single_record")
        rs.next() shouldBe true
        rs.getString(2) shouldBe "alex-new"
        rs.getInt(1) shouldBe 2
        rs.getInt(3) shouldBe 2
      }
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

      val sch = new Schema.Parser().parse(schemaStr)

      val writer = new JdbcRecordWriter(ds, SchemaWrapper.from(sch),
        batchSize = 2, dialect = H2Dialect)
      val rec = new GenericRecordBuilder(sch)
      writer.addBatch(Upsert(rec.set("id", "1").set("username", "alex").build()))

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

    it("fails if updating an existing writer with a different schema name") {
      val schemaStr =
        """
          |{
          |	"type": "record",
          |	"name": "Test1",
          |	"namespace": "hydra",
          |	"fields": [{
          |			"name": "id",
          |			"type": "int"
          |		}
          |	]
          |}""".stripMargin

      val sch = new Schema.Parser().parse(schemaStr)

      val writer = new JdbcRecordWriter(ds, SchemaWrapper.from(sch),
        batchSize = -1, dialect = H2Dialect)

      writer.addBatch(Upsert(new GenericRecordBuilder(sch).set("id", 1).build()))

      val newSchemaStr =
        """
          |{
          |	"type": "record",
          |	"name": "Test1New",
          |	"namespace": "hydra",
          |	"fields": [{
          |			"name": "id",
          |			"type": "int"
          |		}
          |	]
          |}""".stripMargin

      val schN = new Schema.Parser().parse(newSchemaStr)
      intercept[IllegalArgumentException] {
        writer.addBatch(Upsert(new GenericRecordBuilder(schN).set("id", 1).build()))
      }

      writer.close()
    }

    it("fails on deletes") {
      val schemaStr =
        """
          |{
          |	"type": "record",
          |	"name": "DeleteTest",
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


      val writer = new JdbcRecordWriter(ds,
        SchemaWrapper.from(new Schema.Parser().parse(schemaStr)), batchSize = 2, dialect = H2Dialect)
      writer.addBatch(Delete(new Schema.Parser().parse(schemaStr), Map.empty))
    }
  }
}
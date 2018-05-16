package hydra.sql

import java.util.Properties

import com.typesafe.config.ConfigFactory
import hydra.avro.io.{DeleteByKey, SaveMode, Upsert}
import hydra.avro.util.SchemaWrapper
import hydra.common.util.TryWith
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

import scala.concurrent.duration._

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

  val config = ConfigFactory.parseString(
    """
      |connection.url = "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1"
      |connection.user = sa
      |
      """.stripMargin)

  private val writerSettings = JdbcWriterSettings(config)

  val record = new GenericData.Record(schema.schema)
  record.put("id", 1)
  record.put("username", "alex")

  val provider = new DriverManagerConnectionProvider("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1",
    "", "", 1, 1.millis)

  val catalog = new JdbcCatalog(provider, UnderscoreSyntax, H2Dialect)

  override def afterAll() = provider.connection.close()

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
        new JdbcRecordWriter(writerSettings, provider, s, SaveMode.ErrorIfExists)
      }

      new JdbcRecordWriter(writerSettings, provider, s, SaveMode.Append).close()
      new JdbcRecordWriter(writerSettings, provider, s, SaveMode.Overwrite).close()
      new JdbcRecordWriter(writerSettings, provider, s, SaveMode.Ignore).close()
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
      new JdbcRecordWriter(writerSettings, provider, SchemaWrapper.from(s), SaveMode.Append).close()
      catalog.tableExists(TableIdentifier("tester")) shouldBe true
    }

    it("writes") {
      val writer = new JdbcRecordWriter(writerSettings, provider, schema)
      writer.batch(Upsert(record))
      writer.flush()
      val c = provider.getConnection
      val stmt = c.createStatement()
      val rs = stmt.executeQuery("select \"id\",\"username\" from user")
      rs.next()
      Seq(rs.getInt(1), rs.getString(2)) shouldBe Seq(1, "alex")

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


      val writer = new JdbcRecordWriter(writerSettings, provider,
        SchemaWrapper.from(new Schema.Parser().parse(schemaStr)))
      writer.batch(Upsert(record))

      val c1 = provider.getConnection
      TryWith(c1.createStatement()) { stmt =>
        val rs = stmt.executeQuery("select \"id\",\"username\" from flush_test")
        rs.next() shouldBe false
      }.get

      writer.flush()

      TryWith(c1.createStatement()) { stmt =>
        val rs = stmt.executeQuery("select \"id\",\"username\" from flush_test")
        rs.next()
        Seq(rs.getInt(1), rs.getString(2)) shouldBe Seq(1, "alex")
      }.get

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

      val writer = new JdbcRecordWriter(writerSettings, provider,
        SchemaWrapper.from(new Schema.Parser().parse(schemaStr)))

      writer.execute(Upsert(srecord))
      val c = provider.getConnection
      TryWith(c.createStatement()) { stmt =>
        val rs = stmt.executeQuery("select \"id\",\"username\" from write_single_record")
        rs.next() shouldBe true
        rs.getString(2) shouldBe "alex"
        rs.getInt(1) shouldBe 1
      }.get

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

      TryWith(c.createStatement()) { stmt =>
        stmt.executeUpdate("delete from write_single_record")
      }.get
      writer.execute(Upsert(newRecord))

      TryWith(c.createStatement()) { stmt =>
        val rs = stmt.executeQuery("select \"id\",\"username\", \"rank\" from write_single_record")
        rs.next() shouldBe true
        rs.getString(2) shouldBe "alex-new"
        rs.getInt(1) shouldBe 2
        rs.getInt(3) shouldBe 2
      }.get
    }

    it("deletes a single record") {
      val schemaStr =
        """
          |{
          |	"type": "record",
          |	"name": "DeleteSingleRecord",
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
          |		}
          |	]
          |}""".stripMargin

      val pschema = new Schema.Parser().parse(schemaStr)
      val srecord = new GenericData.Record(pschema)
      srecord.put("id", 1)
      srecord.put("username", "alex")

      val writer = new JdbcRecordWriter(writerSettings, provider, SchemaWrapper.from(pschema))

      writer.execute(Upsert(srecord))
      val c = provider.getConnection
      TryWith(c.createStatement()) { stmt =>
        val rs = stmt.executeQuery("select \"id\",\"username\" from delete_single_record")
        rs.next() shouldBe true
        rs.getString(2) shouldBe "alex"
        rs.getInt(1) shouldBe 1
      }.get

      //delete
      writer.execute(DeleteByKey(Map("id" -> (1: java.lang.Integer))))
      TryWith(c.createStatement()) { stmt =>
        val rs = stmt.executeQuery("select \"id\",\"username\" from delete_single_record")
        rs.next() shouldBe false
      }.get
    }

    it("errors gracefully when trying to delete rows from schemas with no PK") {
      val schemaStr =
        """
          |{
          |	"type": "record",
          |	"name": "DeleteError",
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

      val pschema = new Schema.Parser().parse(schemaStr)
      val srecord = new GenericData.Record(pschema)
      srecord.put("id", 1)
      srecord.put("username", "alex")

      val writer = new JdbcRecordWriter(writerSettings, provider, SchemaWrapper.from(pschema))
      intercept[UnsupportedOperationException] {
        writer.execute(DeleteByKey(Map("id" -> (1: java.lang.Integer))))
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


      val writer = new JdbcRecordWriter(writerSettings, provider,
        SchemaWrapper.from(new Schema.Parser().parse(schemaStr)))
      writer.batch(Upsert(record))

      val c = provider.getConnection
      TryWith(c.createStatement()) { stmt =>
        val rs = stmt.executeQuery("select \"id\",\"username\" from flush_on_close")
        rs.next() shouldBe false
      }.get

      writer.close()

      TryWith(c.createStatement()) { stmt =>
        val rs = stmt.executeQuery("select \"id\",\"username\" from flush_on_close")
        rs.next()
        Seq(rs.getInt(1), rs.getString(2)) shouldBe Seq(1, "alex")
      }.get
    }

    it("handles errors in batches") {

      val schemaStr =
        """
          |{
          |	"type": "record",
          |	"name": "BatchFail",
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


      val wrapper = SchemaWrapper.from(new Schema.Parser().parse(schemaStr))
      val writer = new JdbcRecordWriter(writerSettings, provider, wrapper)
      val badRecord = new GenericData.Record(wrapper.schema)
      badRecord.put("id", null)
      badRecord.put("username", "alex")
      val goodRecord = new GenericData.Record(wrapper.schema)
      goodRecord.put("id", 1)
      goodRecord.put("username", "alex1")
      writer.batch(Upsert(goodRecord))
      writer.batch(Upsert(badRecord))

      writer.flush()

      val c = provider.getConnection
      TryWith(c.createStatement()) { stmt =>
        val rs = stmt.executeQuery("select \"id\",\"username\" from batch_fail")
        rs.next() shouldBe true
      }.get

      writer.close()

      TryWith(c.createStatement()) { stmt =>
        val rs = stmt.executeQuery("select \"id\",\"username\" from batch_fail")
        rs.next()
        Seq(rs.getInt(1), rs.getString(2)) shouldBe Seq(1, "alex1")
        rs.next() shouldBe false
      }.get
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


      val writer = new JdbcRecordWriter(writerSettings, provider,
        SchemaWrapper.from(new Schema.Parser().parse(schemaStr)))
      //writer.batch(Delete(Map.empty))
    }

    it("flushes on delete") {
      val schemaStr =
        """
          |{
          |	"type": "record",
          |	"name": "TestFlush",
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
          |		}
          |	]
          |}""".stripMargin

      val c = provider.getConnection

      val pschema = new Schema.Parser().parse(schemaStr)

      val writer = new JdbcRecordWriter(writerSettings, provider,
        SchemaWrapper.from(pschema))

      writer.batch(Upsert(record))

      writer.flush()

      TryWith(c.createStatement()) { stmt =>
        val rs = stmt.executeQuery("select \"id\",\"username\" from test_flush")
        rs.next() shouldBe true
      }.get


      writer.batch(DeleteByKey(Map("id" -> (1: java.lang.Integer))))

      writer.close()

      TryWith(c.createStatement()) { stmt =>
        val rs = stmt.executeQuery("select \"id\",\"username\" from test_flush")
        rs.next() shouldBe false
      }.get
    }

    it("writes to new a versioned table if schema is versioned") {
      val schemaStrV2 = """{
                          |  "type": "record",
                          |  "name": "VersionedTable",
                          |  "namespace": "hydra.v2",
                          |  "fields": [{
                          |    "name": "identifier",
                          |    "type": "int"
                          |  },
                          |  {
                          |    "name": "name",
                          |    "type": "string"
                          |  }]
                          |}""".stripMargin

      val schemaVersion2 = new Schema.Parser().parse(schemaStrV2)

      new JdbcRecordWriter( writerSettings, provider, SchemaWrapper.from(schemaVersion2), SaveMode.Append).close()
      catalog.tableExists(TableIdentifier("versioned_table_v2")) shouldBe true
    }

    it("resets batched operations") {
      val schemaStr =
        """
          |{
          |	"type": "record",
          |	"name": "TestRollback",
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

      val writer = new JdbcRecordWriter(writerSettings, provider,
        SchemaWrapper.from(new Schema.Parser().parse(schemaStr)))
      writer.batch(Upsert(record))
      writer.batch(Upsert(record))
      writer.resetBatchedOps()
      writer.flush()

      val c = provider.getConnection()

      TryWith(c.createStatement()) { stmt =>
        val rs = stmt.executeQuery("select \"id\",\"username\" from test_rollback")
        rs.next() shouldBe false
      }.get
    }
  }
}

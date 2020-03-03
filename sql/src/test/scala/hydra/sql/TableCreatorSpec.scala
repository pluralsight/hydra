package hydra.sql

import hydra.avro.io.SaveMode
import hydra.avro.util.SchemaWrapper
import hydra.common.util.TryWith
import org.apache.avro.Schema
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

import scala.concurrent.duration._

/**
  * Created by alexsilva on 5/4/17.
  */
class TableCreatorSpec
    extends Matchers
    with FunSpecLike
    with BeforeAndAfterAll {

  val provider = new DriverManagerConnectionProvider(
    "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1",
    "",
    "",
    1,
    1.millis
  )

  override def afterAll() = provider.connection.close()

  describe("The TableCreator") {

    it("creates a table that doesn't exist") {
      val schemaStr =
        """
          |{
          |	"type": "record",
          |	"name": "HydraNewTable",
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

      new TableCreator(provider, UnderscoreSyntax, H2Dialect)
        .createOrAlterTable(SaveMode.Append, schema, false)
      TryWith(provider.getConnection().createStatement()) { stmt =>
        val rs =
          stmt.executeQuery("select \"id\",\"username\" from hydra_new_table")
        rs.next() shouldBe false
      }.get
    }

    it("uses the table name when provided") {
      val schemaStr =
        """
          |{
          |	"type": "record",
          |	"name": "HydraDrop",
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

      new TableCreator(provider, UnderscoreSyntax, H2Dialect)
        .createOrAlterTable(
          SaveMode.Overwrite,
          schema,
          false,
          Some(TableIdentifier("another_table"))
        )

      TryWith(provider.getConnection().createStatement()) { stmt =>
        val rs =
          stmt.executeQuery("select \"id\",\"username\" from another_table")
        rs.next() shouldBe false
      }.get

    }

    it("drops a table that already exists") {

      TryWith(provider.getConnection().createStatement()) { stmt =>
        val rs = stmt.executeUpdate(
          "CREATE TABLE hydra_drop (\"id\" INTEGER NOT NULL,\"username\" TEXT ) "
        )
        rs shouldBe 0
        stmt.executeUpdate("""insert into hydra_drop values(1,'test')""") shouldBe 1
      }.get

      val schemaStr =
        """
          |{
          |	"type": "record",
          |	"name": "HydraDrop",
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

      new TableCreator(provider, UnderscoreSyntax, H2Dialect)
        .createOrAlterTable(SaveMode.Overwrite, schema, false)

      TryWith(provider.getConnection().createStatement()) { stmt =>
        val rs = stmt.executeQuery("select \"id\",\"username\" from hydra_drop")
        rs.next() shouldBe false
      }.get

    }

    it("truncates that already exists") {

      TryWith(provider.getConnection().createStatement()) { stmt =>
        val rs = stmt.executeUpdate(
          "CREATE TABLE hydra_truncate (\"id\" INTEGER NOT NULL,\"username\" TEXT ) "
        )
        rs shouldBe 0
        stmt.executeUpdate("""insert into hydra_truncate values(1,'test')""") shouldBe 1
      }.get

      val schemaStr =
        """
          |{
          |	"type": "record",
          |	"name": "HydraTruncate",
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

      new TableCreator(provider, UnderscoreSyntax, H2Dialect)
        .createOrAlterTable(SaveMode.Overwrite, schema, true)

      TryWith(provider.getConnection().createStatement()) { stmt =>
        val rs = stmt.executeQuery("select \"id\",\"username\" from hydra_drop")
        rs.next() shouldBe false
      }.get

    }

    it("appends to a table") {
      TryWith(provider.getConnection().createStatement()) { stmt =>
        val rs = stmt.executeUpdate(
          "CREATE TABLE hydra_append (\"id\" INTEGER NOT NULL,\"username\" TEXT ) "
        )
        rs shouldBe 0
        stmt.executeUpdate("""insert into hydra_append values(1,'test')""") shouldBe 1
      }.get

      val schemaStr =
        """
          |{
          |	"type": "record",
          |	"name": "HydraAppend",
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

      new TableCreator(provider, UnderscoreSyntax, H2Dialect)
        .createOrAlterTable(SaveMode.Append, schema, false)

      TryWith(provider.getConnection().createStatement()) { stmt =>
        val rs =
          stmt.executeQuery("select \"id\",\"username\" from hydra_append")
        rs.next() shouldBe true
      }.get

    }

    it("errors if a table already exists") {
      TryWith(provider.getConnection().createStatement()) { stmt =>
        val rs = stmt.executeUpdate(
          "CREATE TABLE hydra_overwrite (\"id\" INTEGER NOT NULL,\"username\" TEXT ) "
        )
        rs shouldBe 0
        stmt.executeUpdate("""insert into hydra_drop values(1,'test')""") shouldBe 1
      }.get

      val schemaStr =
        """
          |{
          |	"type": "record",
          |	"name": "HydraOverwrite",
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

      intercept[AnalysisException] {
        new TableCreator(provider, UnderscoreSyntax, H2Dialect)
          .createOrAlterTable(SaveMode.ErrorIfExists, schema, false)
      }
    }
  }
}

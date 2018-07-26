package hydra.sql

import com.opentable.db.postgres.embedded.EmbeddedPostgres
import hydra.avro.util.SchemaWrapper
import hydra.common.util.TryWith
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecordBuilder
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

class PostgresUpsertSpec extends Matchers with FlatSpecLike with BeforeAndAfterAll {

  lazy val pg = EmbeddedPostgres.start()

  lazy val pgDb = pg.getPostgresDatabase()


  val compositePKSchema = SchemaWrapper.from(new Schema.Parser().parse(
    """
      |{
      |	"type": "record",
      |	"name": "CompositeKeyUser",
      | "hydra.key":"id,username",
      |	"namespace": "hydra",
      |	"fields": [{
      |			"name": "id",
      |			"type": "int"
      |		},
      |		{
      |			"name": "username",
      |			"type": "string"
      |		},
      |  {
      |			"name": "rank",
      |			"type": "int"
      |		}
      |	]
      |}
    """.stripMargin))

  val schema = SchemaWrapper.from(new Schema.Parser().parse(
    """
      |{
      |	"type": "record",
      |	"name": "SingleKeyUser",
      | "hydra.key":"id",
      |	"namespace": "hydra",
      |	"fields": [{
      |			"name": "id",
      |			"type": "int"
      |		},
      |		{
      |			"name": "username",
      |			"type": "string"
      |		},
      |  {
      |			"name": "rank",
      |			"type": "int"
      |		}
      |	]
      |}
    """.stripMargin))

  override def beforeAll = {
    TryWith(pgDb.getConnection("postgres", "")) { conn =>
      JdbcUtils.createTable(compositePKSchema, PostgresDialect, "test_composite", "", UnderscoreSyntax, conn)
      JdbcUtils.createTable(schema, PostgresDialect, "test_single", "", UnderscoreSyntax, conn)
    }.get
  }

  override def afterAll = {
    TryWith(pgDb.getConnection("postgres", "")) { conn =>
      conn.prepareStatement("drop table test_composite")
      conn.prepareStatement("drop table test_single")
    }
    pg.close()
  }

  "The Postgres dialect" should "create valid upsert statements for composite keys" in {
    TryWith(pgDb.getConnection("postgres", "")) { conn =>
      val sql = PostgresDialect.buildUpsert("test_composite", compositePKSchema, UnderscoreSyntax)
      val stmt = conn.prepareStatement(sql)
      println(sql)
      val rec = new GenericRecordBuilder(compositePKSchema.schema).set("id", 1)
        .set("username", "alex").set("rank", 10).build
      new AvroValueSetter(compositePKSchema, PostgresDialect).bind(rec, stmt)
      stmt.executeUpdate() shouldBe 1
    }.get
  }

  it should "create valid upsert statements for single primary keys" in {
    TryWith(pgDb.getConnection("postgres", "")) { conn =>
      val sql = PostgresDialect.buildUpsert("test_single", schema, UnderscoreSyntax)
      val stmt = conn.prepareStatement(sql)
      println(sql)
      val rec = new GenericRecordBuilder(schema.schema).set("id", 1)
        .set("username", "alex").set("rank", 10).build
      new AvroValueSetter(schema, PostgresDialect).bind(rec, stmt)
      stmt.executeUpdate() shouldBe 1
    }.get
  }

}

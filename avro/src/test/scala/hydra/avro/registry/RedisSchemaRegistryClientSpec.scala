package hydra.avro.registry

import org.apache.avro.{Schema, SchemaBuilder}
import org.scalatest.BeforeAndAfterAll
import com.github.sebruck.EmbeddedRedis
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaMetadata, SchemaRegistryClient}
import net.manub.embeddedkafka.schemaregistry.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import redis.embedded.RedisServer

import scala.collection.JavaConverters._

class RedisSchemaRegistryClientSpec extends AnyFlatSpec with EmbeddedRedis with EmbeddedKafka with BeforeAndAfterAll {

  private var redisClient: SchemaRegistryClient = _
  private var cachedClient: CachedSchemaRegistryClient = _

  private var redis: RedisServer = _
  private var redisPort: Int = _

  implicit val config: EmbeddedKafkaConfig = EmbeddedKafkaConfig()
  override def beforeAll(): Unit = {
    redis = startRedis()
    redisPort = redis.ports().get(0)
    val srUrl = s"http://localhost:${config.schemaRegistryPort}"
    redisClient = new RedisSchemaRegistryClient(srUrl, "localhost", redisPort)
    cachedClient = new CachedSchemaRegistryClient(srUrl, 50)
  }


  "RedisSchemaRegistryClient" should "successfully pass tests with SchemaRegistry and Redis" in {
    withRunningKafka {
      withRedis() { _ =>

        val topicName1 = "topicA"
        val topicName2 = "topicB"

        val schema1: Schema = SchemaBuilder.record("Test1").fields()
          .requiredString("testing1").endRecord()

        val schema12: Schema = SchemaBuilder.record("Test12").fields()
          .requiredString("testing1").endRecord()

        val schema2: Schema = SchemaBuilder.record("Test2").fields()
          .requiredString("testing2").endRecord()

        //register two schemas with different clients
        val redisClientResult = redisClient.register(topicName1, schema1)
        val cachedClientResult = cachedClient.register(topicName2 ,schema2)

        //test the getAllSubjectsById method
        assert(redisClient.getAllSubjectsById(redisClientResult).contains(topicName1))
        assert(cachedClient.getAllSubjectsById(redisClientResult).contains(topicName1))
        assert(redisClient.getAllSubjectsById(cachedClientResult).contains(topicName2))
        assert(cachedClient.getAllSubjectsById(cachedClientResult).contains(topicName2))

        //test the getById method
        assert(redisClient.getById(redisClientResult).equals(schema1))
        assert(cachedClient.getById(redisClientResult).equals(schema1))
        assert(redisClient.getById(cachedClientResult).equals(schema2))
        assert(cachedClient.getById(cachedClientResult).equals(schema2))


        //test the getBySubjectAndId method
        Thread.sleep(3000)
        assert(redisClient.getBySubjectAndId(topicName1, redisClientResult).equals(schema1))
        Thread.sleep(3000)
        assert(cachedClient.getBySubjectAndId(topicName1, redisClientResult).equals(schema1))
        Thread.sleep(3000)
        assert(redisClient.getBySubjectAndId(topicName2, cachedClientResult).equals(schema2))
        Thread.sleep(3000)
        assert(cachedClient.getBySubjectAndId(topicName2, cachedClientResult).equals(schema2))


        //test the getAllSubjects method
        val rAllSubjects = redisClient.getAllSubjects
        val cAllSubjects = cachedClient.getAllSubjects
        assert(rAllSubjects.size() == cAllSubjects.size() && rAllSubjects.size() == 2)
        assert(rAllSubjects.containsAll(List(topicName1, topicName2).asJava))
        assert(cAllSubjects.containsAll(List(topicName1, topicName2).asJava))

        //test the getAllVersions method
        val gav1 = redisClient.getAllVersions(topicName1)
        val gav2 = cachedClient.getAllVersions(topicName1)
        val gav12 = redisClient.getAllVersions(topicName2)
        val gav22 = cachedClient.getAllVersions(topicName2)
        assert(gav1 == List(1).asJava)
        assert(gav2 == gav1)
        assert(gav12 == List(1).asJava)
        assert(gav22 == gav12)

        //test the getId method
        assert(redisClient.getId(topicName1, schema1) == redisClientResult)
        assert(cachedClient.getId(topicName1, schema1) == redisClientResult)
        assert(redisClient.getId(topicName2, schema2) == cachedClientResult)
        assert(cachedClient.getId(topicName2, schema2) == cachedClientResult)

        //test the getLatestSchemaMetadata method
        val schemaMetadata1 = new SchemaMetadata(1, 1,schema1.toString)
        val schemaMetadata2 = new SchemaMetadata(3, 2,schema12.toString)

        val schemaMetadata1Result = redisClient.getLatestSchemaMetadata(topicName1)
        schemaMetadata1Result.getId shouldBe schemaMetadata1.getId
        schemaMetadata1Result.getVersion shouldBe schemaMetadata1.getVersion
        schemaMetadata1Result.getSchema shouldBe schemaMetadata1.getSchema

        redisClient.register(topicName1, schema12)
        val schemaMetadata2Result = redisClient.getLatestSchemaMetadata(topicName1)
        schemaMetadata2Result.getId shouldBe schemaMetadata2.getId
        schemaMetadata2Result.getVersion shouldBe schemaMetadata2.getVersion
        schemaMetadata2Result.getSchema shouldBe schemaMetadata2.getSchema

        succeed
      }
    }
  }

  override def afterAll(): Unit = {
    stopRedis(redis)
  }
}

package hydra.avro.registry

import org.apache.avro.{Schema, SchemaBuilder}
import org.scalatest.BeforeAndAfterAll
import com.github.sebruck.EmbeddedRedis
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException
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
        redisClient.getById(redisClientResult) shouldBe schema1
        cachedClient.getById(redisClientResult) shouldBe schema1
        redisClient.getById(cachedClientResult) shouldBe schema2
        cachedClient.getById(cachedClientResult) shouldBe schema2

        //test the getBySubjectAndId method
        Thread.sleep(3000)
        assert(redisClient.getBySubjectAndId(topicName1, redisClientResult).equals(schema1))
        assert(cachedClient.getBySubjectAndId(topicName1, redisClientResult).equals(schema1))
        assert(redisClient.getBySubjectAndId(topicName2, cachedClientResult).equals(schema2))
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
        gav1 shouldBe List(1).asJava
        gav2 shouldBe gav1
        gav12 shouldBe List(1).asJava
        gav22 shouldBe gav12

        //test the getId method
        Thread.sleep(3000)
        redisClient.getId(topicName1, schema1) shouldBe redisClientResult
        cachedClient.getId(topicName1, schema1) shouldBe redisClientResult
        redisClient.getId(topicName2, schema2) shouldBe cachedClientResult
        cachedClient.getId(topicName2, schema2) shouldBe cachedClientResult

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

        //test the getSchemaMetadata method
        val metadata1 = redisClient.getSchemaMetadata(topicName1, 1)
        metadata1.getId shouldBe schemaMetadata1.getId
        metadata1.getVersion shouldBe schemaMetadata1.getVersion
        metadata1.getSchema shouldBe schemaMetadata1.getSchema

        val metadata2 = cachedClient.getSchemaMetadata(topicName2, 1)
        val _schemaMetadata2 = new SchemaMetadata(2, 1,schema2.toString)
        metadata2.getId shouldBe _schemaMetadata2.getId
        metadata2.getVersion shouldBe _schemaMetadata2.getVersion
        metadata2.getSchema shouldBe _schemaMetadata2.getSchema

        //test the getVersion method
        Thread.sleep(3000)
        redisClient.getVersion(topicName1, schema1) shouldBe 1
        redisClient.getVersion(topicName1, schema12) shouldBe 2
        redisClient.getVersion(topicName2, schema2) shouldBe 1

        //test the deleteSchemaVersion method
        //the latest metadata is in this test -> test the getLatestSchemaMetadata method
        redisClient.deleteSchemaVersion(topicName1, 2.toString)
        val schemaMetadataResult2 = redisClient.getLatestSchemaMetadata(topicName1)
        schemaMetadataResult2.getId shouldBe schemaMetadata1.getId
        schemaMetadataResult2.getVersion shouldBe schemaMetadata1.getVersion
        schemaMetadataResult2.getSchema shouldBe schemaMetadata1.getSchema

        //test the deleteSchemaVersion 2 method
        redisClient.deleteSchemaVersion(RedisSchemaRegistryClient.DEFAULT_REQUEST_PROPERTIES.asJava, topicName2, 1.toString)

        intercept[RestClientException] {
          redisClient.getLatestSchemaMetadata(topicName2)
        }.getMessage shouldBe "Subject not found.; error code: 40401"

        succeed
      }
    }
  }

  override def afterAll(): Unit = {
    stopRedis(redis)
  }
}

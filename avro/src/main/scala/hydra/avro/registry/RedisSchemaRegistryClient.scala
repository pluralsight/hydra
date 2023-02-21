package hydra.avro.registry

import io.confluent.kafka.schemaregistry.client.{SchemaMetadata, SchemaRegistryClient}
import io.confluent.kafka.schemaregistry.client.rest.RestService
import io.confluent.kafka.schemaregistry.client.security.SslFactory
import org.apache.avro.Schema
import scalacache._
import scalacache.redis._
import scalacache.modes.try_._
import scalacache.serialization.Codec

import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

object RedisSchemaRegistryClient {

  import com.twitter.chill.KryoInjection

  private val schemaCacheCodec: Codec[Map[Schema, Int]] = new Codec[Map[Schema, Int]] {

    def encode(value: Map[Schema, Int]): Array[Byte] = KryoInjection(value)

    def decode(bytes: Array[Byte]): Codec.DecodingResult[Map[Schema, Int]] = {
      Codec.tryDecode(KryoInjection.invert(bytes).map(_.asInstanceOf[Map[Schema, Int]]).getOrElse(Map.empty[Schema, Int]))
    }
  }

  private val idCacheCodec: Codec[Map[Int, Schema]] = new Codec[Map[Int, Schema]] {

    def encode(value: Map[Int, Schema]): Array[Byte] = KryoInjection(value)

    def decode(bytes: Array[Byte]): Codec.DecodingResult[Map[Int, Schema]] = {
      Codec.tryDecode(KryoInjection.invert(bytes).map(_.asInstanceOf[Map[Int, Schema]]).getOrElse(Map.empty[Int, Schema]))
    }
  }

  private val idCacheConfig = CacheConfig.defaultCacheConfig
  private val schemaCacheConfig = CacheConfig.defaultCacheConfig
  private val versionCacheConfig = CacheConfig.defaultCacheConfig

  private val idCacheDurationTtl = Option(Duration(1, TimeUnit.SECONDS))
  private val schemaCacheDurationTtl = Option(Duration(1, TimeUnit.SECONDS))
  private val versionCacheDurationTtl = Option(Duration(1, TimeUnit.SECONDS))
}

class RedisSchemaRegistryClient(restService: RestService,
                                redisHost: String,
                                redisPort: Int,
                                httpHeaders: Map[String, String],
                                configs: Map[String, Any]) extends SchemaRegistryClient {

  def this(baseUrl: String, redisHost: String, redisPort: Int) {
    this(new RestService(baseUrl), redisHost, redisPort, Map.empty[String, String], Map.empty[String, Any])
  }

  def this(baseUrl: String, redisHost: String, redisPort: Int, config: Map[String, Any]) {
    this(new RestService(baseUrl), redisHost, redisPort, Map.empty[String, String], config)
  }

  def this(baseUrl: String, httpHeaders: Map[String, String], redisHost: String, redisPort: Int) {
    this(new RestService(baseUrl), redisHost, redisPort, httpHeaders, Map.empty[String, Any])
  }

  def this(baseUrl: String, redisHost: String, redisPort: Int, httpHeaders: Map[String, String], config: Map[String, Any]) {
    this(new RestService(baseUrl), redisHost, redisPort, httpHeaders, config)
  }

  def this(baseUrls: List[String], redisHost: String, redisPort: Int) {
    this(new RestService(baseUrls.asJava), redisHost, redisPort, Map.empty[String, String], Map.empty[String, Any])
  }

  def this(baseUrls: List[String], redisHost: String, redisPort: Int, config: Map[String, Any]) {
    this(new RestService(baseUrls.asJava), redisHost, redisPort, Map.empty[String, String], config)
  }

  def this(baseUrls: List[String], httpHeaders: Map[String, String], redisHost: String, redisPort: Int) {
    this(new RestService(baseUrls.asJava), redisHost, redisPort, httpHeaders, Map.empty[String, Any])
  }

  def this(baseUrls: List[String], redisHost: String, redisPort: Int, httpHeaders: Map[String, String], config: Map[String, Any]) {
    this(new RestService(baseUrls.asJava), redisHost, redisPort, httpHeaders, config)
  }

  import hydra.avro.registry.RedisSchemaRegistryClient._

  private val DEFAULT_REQUEST_PROPERTIES = Map("Content-Type" -> "application/vnd.schemaregistry.v1+json")

  private val schemaCache: Cache[Map[Schema, Int]] =
    RedisCache(redisHost, redisPort)(schemaCacheConfig, schemaCacheCodec)

  private val idCache: Cache[Map[Int, Schema]] =
    RedisCache(redisHost, redisPort)(idCacheConfig, idCacheCodec)

  private val versionCache: Cache[Map[Schema, Int]] =
    RedisCache(redisHost, redisPort)(versionCacheConfig, schemaCacheCodec)

  if (httpHeaders.nonEmpty) {
    restService.setHttpHeaders(httpHeaders.asJava)
  }

  if (configs.nonEmpty && configs.nonEmpty) {
    restService.configure(configs.asJava)

    val srPrefix = "schema.registry."

    val sslConfigs = configs
      .filter(kv => kv._1.startsWith(srPrefix))
      .map {
        kv =>
          (kv._1.substring(srPrefix.length), kv._2)
      }

    val sslFactory: SslFactory = new SslFactory(sslConfigs.asJava)

    if (sslFactory != null && sslFactory.sslContext() != null) {
      restService.setSslSocketFactory(sslFactory.sslContext().getSocketFactory)
    }
  }

  override def register(s: String, schema: Schema): Int = {
    register(s, schema, 0, -1)
  }
  override def getById(i: Int): Schema = synchronized {
    val restSchema = restService.getId(i)
    val parser = new Schema.Parser()
    parser.setValidateDefaults(false)
    parser.parse(restSchema.getSchemaString)
  }

  override def getBySubjectAndId(s: String, i: Int): Schema = synchronized {

    def call(): Map[Int, Schema] = Try {
      val restSchema = restService.getId(i)
      val parser = new Schema.Parser()
      parser.setValidateDefaults(false)
      val schema = parser.parse(restSchema.getSchemaString)
      Map(i -> schema)
    }.getOrElse(Map.empty)

    idCache.get(s) match {
      case Failure(_) =>
        val map = call()
        idCache.put(s)(map, idCacheDurationTtl)
        map(i)
      case Success(m) if m.nonEmpty && m.head.keys.exists(_ == i) =>
        m.head(i)
      case Success(m) =>
        val map = call()
        idCache.put(s)(m.getOrElse(Map.empty) ++ map, idCacheDurationTtl)
        map(i)
    }
  }

  override def getAllSubjectsById(i: Int): java.util.Collection[String] = {
    restService.getAllSubjectsById(i)
  }

  override def getLatestSchemaMetadata(s: String): SchemaMetadata = synchronized {
    val response = restService.getLatestVersion(s)
    val id = response.getId
    val schema: String  = response.getSchema
    val version = response.getVersion
    new SchemaMetadata(id, version, schema)
  }

  override def getSchemaMetadata(s: String, i: Int): SchemaMetadata = {
    val response = restService.getVersion(s, i)
    val id = response.getId
    val schema: String = response.getSchema
    new SchemaMetadata(id, i, schema)
  }

  override def getVersion(s: String, schema: Schema): Int = synchronized {
    def call(): Map[Schema, Int] = Try {
      val response = restService.lookUpSubjectVersion(schema.toString(), s, true)
      Map(schema -> response.getVersion.toInt)
    }.getOrElse(Map.empty)

    versionCache.get(s) match {
      case Failure(_) =>
        val map = call()
        versionCache.put(s)(map, versionCacheDurationTtl)
        map(schema)
      case Success(m) if m.nonEmpty && m.head.keys.exists(_ == schema) =>
        m.head(schema)
      case Success(m) =>
        val map = call()
        versionCache.put(s)(m.getOrElse(Map.empty) ++ map, versionCacheDurationTtl)
        map(schema)
    }
  }

  override def getAllVersions(s: String): java.util.List[Integer] = {
    restService.getAllVersions(s)
  }

  override def updateCompatibility(s: String, s1: String): String = {
    restService.updateCompatibility(s1, s).getCompatibilityLevel
  }

  override def getCompatibility(s: String): String = {
    restService.getConfig(s).getCompatibilityLevel
  }

  override def setMode(s: String): String = {
    restService.setMode(s).getMode
  }

  override def setMode(s: String, s1: String): String = {
    restService.setMode(s, s1).getMode
  }

  override def getMode: String = {
    restService.getMode().getMode
  }

  override def getMode(s: String): String = {
    restService.getMode(s).getMode
  }

  override def getAllSubjects: java.util.Collection[String] = {
    restService.getAllSubjects()
  }

  override def getId(s: String, schema: Schema): Int = synchronized {

    def call(): Map[Schema, Int] = Try{
      val response = restService.lookUpSubjectVersion(schema.toString, s, false)
      Map(schema -> response.getId.toInt)
    }.getOrElse(Map.empty)

    def populateVersionCache(m: Map[Schema, Int]): Try[Any] = {
      val idM = m.map(kv => kv._2 -> kv._1)

      idCache.get(s) match {
        case Failure(_) =>
          idCache.put(s)(idM, idCacheDurationTtl)
        case Success(im) =>
          idCache.put(s)(idM ++ im.getOrElse(Map.empty), idCacheDurationTtl)
      }
    }

    schemaCache.get(s) match {
      case Failure(_) =>
        val map = call()
        schemaCache.put(s)(map, schemaCacheDurationTtl)
        map(schema)
      case Success(m) if m.nonEmpty && m.head.keys.exists(_ == schema) =>
        populateVersionCache(m.head)
        m.head(schema)
      case Success(m) =>
        val map = call()
        populateVersionCache(m.getOrElse(Map.empty) ++ map)
        schemaCache.put(s)(m.getOrElse(Map.empty) ++ map, schemaCacheDurationTtl)
        map(schema)
    }
  }

  override def deleteSubject(s: String): java.util.List[Integer] = synchronized {
    deleteSubject(DEFAULT_REQUEST_PROPERTIES.asJava, s)
  }

  override def deleteSubject(map: java.util.Map[String, String], s: String): java.util.List[Integer] = synchronized {
    versionCache.remove(s)
    idCache.remove(s)
    schemaCache.remove(s)
    restService.deleteSubject(map, s)
  }

  override def deleteSchemaVersion(s: String, s1: String): Integer = {
    deleteSchemaVersion(DEFAULT_REQUEST_PROPERTIES.asJava, s, s1)
  }

  override def deleteSchemaVersion(map: java.util.Map[String, String], s: String, s1: String): Integer = synchronized {
    Try(restService.deleteSchemaVersion(map, s, s1)) match {
      case Failure(exception) => throw exception
      case Success(value) =>
        val cacheMap: Map[Schema, Int] = versionCache.get(s).map(_.get).getOrElse(Map.empty[Schema, Int])

        if (cacheMap.nonEmpty) {
          if (cacheMap.values.exists(_ == value)) {
            versionCache.put(s)(cacheMap.filterNot(kv => kv._2 == value), versionCacheDurationTtl)
          }
        }
        value
    }
  }

  override def register(s: String, schema: Schema, i: Int, i1: Int): Int = synchronized {

    def register(): Int = Try {
      if (i >= 0) {
        restService.registerSchema(schema.toString(), s, i, i1)
      } else {
        restService.registerSchema(schema.toString(), s)
      }
    }.getOrElse(-1)

    def populateIdCache(sc: Schema, id: Int): Any = {
      idCache.get(s) match {
        case Failure(_) =>
          idCache.put(s)(Map(id -> sc), idCacheDurationTtl)
        case Success(m) if m.nonEmpty && m.head.exists(_ == (id -> sc)) =>
          ()
        case Success(m) =>
          idCache.put(s)(Map(id -> sc) ++ m.getOrElse(Map.empty), idCacheDurationTtl)
      }
    }

    schemaCache.get(s) match {
      case Failure(_) =>
        val retrievedId = register()
        schemaCache.put(s)(Map(schema -> retrievedId), schemaCacheDurationTtl)
        populateIdCache(schema, retrievedId)
        retrievedId
      case Success(m) if m.nonEmpty && m.head.exists(_._1 == schema) =>
        val cachedId = m.head(schema)
        if (i1 >= 0 && i1 != cachedId) {
          throw new IllegalStateException("Schema already registered with id " + cachedId + " instead of input id " + i1)
        } else {
          cachedId
        }

      case Success(m) =>
        val retrievedId = register()
        schemaCache.put(s)(Map(schema -> retrievedId) ++ m.getOrElse(Map.empty), schemaCacheDurationTtl)
        populateIdCache(schema, retrievedId)
        retrievedId
    }
  }

  override def reset(): Unit =
    throw new UnsupportedOperationException("The reset operation unsupported for a distributed cache.")

  override def getByID(i: Int): Schema = {
    getById(i)
  }

  override def getBySubjectAndID(s: String, i: Int): Schema = {
    getBySubjectAndId(s, i)
  }

  override def testCompatibility(s: String, schema: Schema): Boolean = {
    restService.testCompatibility(schema.toString(), s, "latest")
  }

}

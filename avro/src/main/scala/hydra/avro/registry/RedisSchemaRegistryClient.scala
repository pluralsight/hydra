package hydra.avro.registry

import com.typesafe.config.{Config, ConfigFactory}
import hydra.common.config.ConfigSupport.ConfigImplicits
import hydra.common.config.KafkaConfigUtils.SchemaRegistrySecurityConfig
import io.confluent.kafka.schemaregistry.client.{SchemaMetadata, SchemaRegistryClient}
import io.confluent.kafka.schemaregistry.client.rest.RestService
import io.confluent.kafka.schemaregistry.client.security.SslFactory
import org.apache.avro.Schema
import scalacache._
import scalacache.redis._
import scalacache.modes.try_._
import scalacache.serialization.Codec
import scalacache.serialization.Codec.DecodingResult

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

case class CacheConfigs(idCacheTtl: Int, schemaCacheTtl: Int, versionCacheTtl: Int)

object RedisSchemaRegistryClient {

  object SchemaIntMapBinCodec extends Codec[Map[Schema, Int]] {
    override def encode(value: Map[Schema, Int]): Array[Byte] = {
      val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
      val oos = new ObjectOutputStream(stream)
      oos.writeObject(value)
      oos.close()
      stream.toByteArray
    }

    override def decode(bytes: Array[Byte]): DecodingResult[Map[Schema, Int]] = {
      val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
      val value = ois.readObject
      ois.close()
      Codec.tryDecode(value.asInstanceOf[Map[Schema, Int]])
    }
  }

  object IntSchemaMapBinCodec extends Codec[Map[Int, Schema]] {
    override def encode(value: Map[Int, Schema]): Array[Byte] = {
      val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
      val oos = new ObjectOutputStream(stream)
      oos.writeObject(value)
      oos.close()
      stream.toByteArray
    }

    override def decode(bytes: Array[Byte]): DecodingResult[Map[Int, Schema]] = {
      val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
      val value = ois.readObject
      ois.close()
      Codec.tryDecode(value.asInstanceOf[Map[Int, Schema]])
    }
  }

  val DEFAULT_REQUEST_PROPERTIES = Map("Content-Type" -> "application/vnd.schemaregistry.v1+json")

  private val schemaCacheCodec: Codec[Map[Schema, Int]] = new Codec[Map[Schema, Int]] {

    def encode(value: Map[Schema, Int]): Array[Byte] = SchemaIntMapBinCodec.encode(value)

    def decode(bytes: Array[Byte]): Codec.DecodingResult[Map[Schema, Int]] = SchemaIntMapBinCodec.decode(bytes)
  }

  private val idCacheCodec: Codec[Map[Int, Schema]] = new Codec[Map[Int, Schema]] {

    def encode(value: Map[Int, Schema]): Array[Byte] = IntSchemaMapBinCodec.encode(value)

    def decode(bytes: Array[Byte]): Codec.DecodingResult[Map[Int, Schema]] = IntSchemaMapBinCodec.decode(bytes)
  }

  private def readIntConfigParameter(config: Config, path: String): Int = {
    config.getIntOpt(path)
      .getOrElse(throw new IllegalArgumentException(s"A $path is required."))
  }

  private def readStringConfigParameter(config: Config, path: String): String = {
    config.getStringOpt(path)
      .getOrElse(throw new IllegalArgumentException(s"A $path is required."))
  }

  def registryUrl(config: Config): String =
    readStringConfigParameter(config, "schema.registry.url")

  def forConfig(
                 config: Config = ConfigFactory.load(),
                 schemaRegistrySecurityConfig: SchemaRegistrySecurityConfig
               ): SchemaRegistryComponent = {
    val baseUrl = registryUrl(config)
    val redisHost = readStringConfigParameter(config, "schema.registry.redis.host")
    val redisPort = readIntConfigParameter(config, "schema.registry.redis.port")
    val redisIdCacheTtl = readIntConfigParameter(config, "schema.registry.redis.id-cache-ttl")
    val redisSchemaCacheTtl = readIntConfigParameter(config, "schema.registry.redis.schema-cache-ttl")
    val redisVersionCacheTtl = readIntConfigParameter(config, "schema.registry.redis.version-cache-ttl")

    val client = new RedisSchemaRegistryClient(
      baseUrl,
      redisHost,
      redisPort,
      schemaRegistrySecurityConfig.toConfigMap,
      CacheConfigs(redisIdCacheTtl, redisSchemaCacheTtl, redisVersionCacheTtl)
    )

    new SchemaRegistryComponent {
      override def registryClient: SchemaRegistryClient = client

      override def registryUrl: String = baseUrl
    }
  }
}

class RedisSchemaRegistryClient(restService: RestService,
                                redisHost: String,
                                redisPort: Int,
                                httpHeaders: Map[String, String],
                                configs: Map[String, Any],
                                cacheConfigs: CacheConfigs) extends SchemaRegistryClient {

  def this(baseUrl: String, redisHost: String, redisPort: Int) {
    this(new RestService(baseUrl), redisHost, redisPort, Map.empty[String, String], Map.empty[String, Any],  CacheConfigs(1, 1, 1))
  }

  def this(baseUrl: String, redisHost: String, redisPort: Int, cacheConfigs: CacheConfigs) {
    this(new RestService(baseUrl), redisHost, redisPort, Map.empty[String, String], Map.empty[String, Any], cacheConfigs)
  }

  def this(baseUrl: String, redisHost: String, redisPort: Int, config: Map[String, Any]) {
    this(new RestService(baseUrl), redisHost, redisPort, Map.empty[String, String], config, CacheConfigs(1, 1, 1))
  }

  def this(baseUrl: String, redisHost: String, redisPort: Int, config: Map[String, Any], cacheConfigs: CacheConfigs) {
    this(new RestService(baseUrl), redisHost, redisPort, Map.empty[String, String], config, cacheConfigs)
  }

  def this(baseUrl: String, httpHeaders: Map[String, String], redisHost: String, redisPort: Int) {
    this(new RestService(baseUrl), redisHost, redisPort, httpHeaders, Map.empty[String, Any], CacheConfigs(1, 1, 1))
  }

  def this(baseUrl: String, httpHeaders: Map[String, String], redisHost: String, redisPort: Int, cacheConfigs: CacheConfigs) {
    this(new RestService(baseUrl), redisHost, redisPort, httpHeaders, Map.empty[String, Any], cacheConfigs)
  }

  def this(baseUrl: String, redisHost: String, redisPort: Int, httpHeaders: Map[String, String], config: Map[String, Any]) {
    this(new RestService(baseUrl), redisHost, redisPort, httpHeaders, config, CacheConfigs(1, 1, 1))
  }

  def this(baseUrl: String, redisHost: String, redisPort: Int, httpHeaders: Map[String, String], config: Map[String, Any], cacheConfigs: CacheConfigs) {
    this(new RestService(baseUrl), redisHost, redisPort, httpHeaders, config, cacheConfigs)
  }

  def this(baseUrls: List[String], redisHost: String, redisPort: Int) {
    this(new RestService(baseUrls.asJava), redisHost, redisPort, Map.empty[String, String], Map.empty[String, Any], CacheConfigs(1, 1, 1))
  }

  def this(baseUrls: List[String], redisHost: String, redisPort: Int, cacheConfigs: CacheConfigs) {
    this(new RestService(baseUrls.asJava), redisHost, redisPort, Map.empty[String, String], Map.empty[String, Any], cacheConfigs)
  }

  def this(baseUrls: List[String], redisHost: String, redisPort: Int, config: Map[String, Any]) {
    this(new RestService(baseUrls.asJava), redisHost, redisPort, Map.empty[String, String], config, CacheConfigs(1, 1, 1))
  }

  def this(baseUrls: List[String], redisHost: String, redisPort: Int, config: Map[String, Any], cacheConfigs: CacheConfigs) {
    this(new RestService(baseUrls.asJava), redisHost, redisPort, Map.empty[String, String], config, cacheConfigs)
  }

  def this(baseUrls: List[String], httpHeaders: Map[String, String], redisHost: String, redisPort: Int) {
    this(new RestService(baseUrls.asJava), redisHost, redisPort, httpHeaders, Map.empty[String, Any], CacheConfigs(1, 1, 1))
  }

  def this(baseUrls: List[String], httpHeaders: Map[String, String], redisHost: String, redisPort: Int, cacheConfigs: CacheConfigs) {
    this(new RestService(baseUrls.asJava), redisHost, redisPort, httpHeaders, Map.empty[String, Any], cacheConfigs)
  }

  def this(baseUrls: List[String], redisHost: String, redisPort: Int, httpHeaders: Map[String, String], config: Map[String, Any]) {
    this(new RestService(baseUrls.asJava), redisHost, redisPort, httpHeaders, config, CacheConfigs(1, 1, 1))
  }

  def this(baseUrls: List[String], redisHost: String, redisPort: Int, httpHeaders: Map[String, String], config: Map[String, Any], cacheConfigs: CacheConfigs) {
    this(new RestService(baseUrls.asJava), redisHost, redisPort, httpHeaders, config, cacheConfigs)
  }

  import hydra.avro.registry.RedisSchemaRegistryClient._

  private val idCacheConfig = CacheConfig.defaultCacheConfig
  private val schemaCacheConfig = CacheConfig.defaultCacheConfig
  private val versionCacheConfig = CacheConfig.defaultCacheConfig

  private val idCacheDurationTtl = Option(Duration(cacheConfigs.idCacheTtl, TimeUnit.SECONDS))
  private val schemaCacheDurationTtl = Option(Duration(cacheConfigs.schemaCacheTtl, TimeUnit.SECONDS))
  private val versionCacheDurationTtl = Option(Duration(cacheConfigs.versionCacheTtl, TimeUnit.SECONDS))

  private val schemaCache: Cache[Map[Schema, Int]] =
    RedisCache(redisHost, redisPort)(schemaCacheConfig, schemaCacheCodec)

  private val idCache: Cache[Map[Int, Schema]] =
    RedisCache(redisHost, redisPort)(idCacheConfig, idCacheCodec)

  private val versionCache: Cache[Map[Schema, Int]] =
    RedisCache(redisHost, redisPort)(versionCacheConfig, schemaCacheCodec)

  if (httpHeaders.nonEmpty) {
    restService.setHttpHeaders(httpHeaders.asJava)
  }

  if (configs!= null && configs.nonEmpty) {
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
        call()(i)
      case Success(map) =>
        map match {
          case Some(m) if m.keys.exists(_ == i) =>
            m(i)
          case Some(m) =>
            val c = call()
            idCache.put(s)(m ++ c, idCacheDurationTtl)
            c(i)
          case None =>
            val c = call()
            idCache.put(s)(c, idCacheDurationTtl)
            c(i)
        }
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
    versionCache.get(s) match {
      case Failure(_) =>
        val response: io.confluent.kafka.schemaregistry.client.rest.entities.Schema = restService.lookUpSubjectVersion(schema.toString(), s, true)
        versionCache.put(s)(Map(schema -> response.getVersion.toInt), versionCacheDurationTtl)
        response.getVersion.toInt
      case Success(map) =>
        map match {
          case Some(m) if m.keySet.contains(schema) =>
            m(schema)
          case Some(m) =>
            val response: io.confluent.kafka.schemaregistry.client.rest.entities.Schema = restService.lookUpSubjectVersion(schema.toString(), s, true)
            versionCache.put(s)(m ++ Map(schema -> response.getVersion.toInt), versionCacheDurationTtl)
            response.getVersion.toInt
          case None =>
            val response: io.confluent.kafka.schemaregistry.client.rest.entities.Schema = restService.lookUpSubjectVersion(schema.toString(), s, true)
            versionCache.put(s)(Map(schema -> response.getVersion.toInt), versionCacheDurationTtl)
            response.getVersion.toInt
        }
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

    def call(): Map[Schema, Int] = {
      val response = restService.lookUpSubjectVersion(schema.toString, s, false)
      Map(schema -> response.getId.toInt)
    }

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
        schemaCache.put(s)(call(), schemaCacheDurationTtl)
        call()(schema)
      case Success(map) => map match {
        case Some(m) if m.keys.exists(_ == schema) =>
          populateVersionCache(m)
          m(schema)
        case Some(m) =>
          val map = call()
          populateVersionCache(m ++ map)
          schemaCache.put(s)(m ++ map, schemaCacheDurationTtl)
          call()(schema)
        case None =>
          schemaCache.put(s)(call(), schemaCacheDurationTtl)
          call()(schema)
      }
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
      idCache.caching(s)(idCacheDurationTtl) {
        Map(id -> sc)
      } match {
        case Failure(_) => idCache.put(s)(Map(id -> sc), idCacheDurationTtl)
        case Success(m) if m.exists(_ == (id -> sc)) => ()
        case Success(m) => idCache.put(s)(Map(id -> sc) ++ m, idCacheDurationTtl)
      }
    }

    schemaCache.get(s) match {
      case Failure(_) =>
        val retrievedId = register()
        populateIdCache(schema, retrievedId)
        retrievedId
      case Success(map) => map match {
        case Some(m) if m.exists(_._1 == schema) =>
          val cachedId = m(schema)

          if (i1 >= 0 && i1 != cachedId) {
            throw new IllegalStateException("Schema already registered with id " + cachedId + " instead of input id " + i1)
          } else {
            cachedId
          }
        case Some(m) =>
          val retrievedId = register()
          schemaCache.put(s)(Map(schema -> retrievedId) ++ m, schemaCacheDurationTtl)
          populateIdCache(schema, retrievedId)
          retrievedId
        case None =>
          val retrievedId = register()
          populateIdCache(schema, retrievedId)
          retrievedId
      }
    }
  }

  override def reset(): Unit =
    println("The reset operation unsupported for a distributed cache.")

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

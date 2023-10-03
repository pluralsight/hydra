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
import _root_.redis.clients.jedis._

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

case class CacheConfigs(idCacheTtl: Int, schemaCacheTtl: Int, versionCacheTtl: Int, metadataCacheTtl: Option[Int] = Option(12000))

object RedisSchemaRegistryClient {

  private val DO_NOT_CACHE_LIST = List("_", "hydra.")

  object SchemaIntMapBinCodec extends Codec[Map[Schema, Int]] {
    override def encode(value: Map[Schema, Int]): Array[Byte] = {
      val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
      val oos = new ObjectOutputStream(stream)
      oos.writeObject(value)
      oos.close()
      stream.close()
      stream.toByteArray
    }

    override def decode(bytes: Array[Byte]): DecodingResult[Map[Schema, Int]] = {
      val stream = new ByteArrayInputStream(bytes)
      val ois = new ObjectInputStream(stream)
      val value = ois.readObject
      ois.close()
      stream.close()
      Codec.tryDecode(value.asInstanceOf[Map[Schema, Int]])
    }
  }

  object IntSchemaMapBinCodec extends Codec[Map[Int, Schema]] {
    override def encode(value: Map[Int, Schema]): Array[Byte] = {
      val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
      val oos = new ObjectOutputStream(stream)
      oos.writeObject(value)
      oos.close()
      stream.close()
      stream.toByteArray
    }

    override def decode(bytes: Array[Byte]): DecodingResult[Map[Int, Schema]] = {
      val stream = new ByteArrayInputStream(bytes)
      val ois = new ObjectInputStream(stream)
      val value = ois.readObject
      ois.close()
      stream.close()
      Codec.tryDecode(value.asInstanceOf[Map[Int, Schema]])
    }
  }

  private object SerializableSchemaMetadata {
    def fromSchemaMetadata(sm: SchemaMetadata): SerializableSchemaMetadata = {
      new SerializableSchemaMetadata(sm.getId, sm.getVersion, sm.getSchema)
    }
  }
  private class SerializableSchemaMetadata(id: Int, version: Int, schema: String) extends Serializable {
    def getSchemaMetadata: SchemaMetadata = {
      new SchemaMetadata(id, version, schema)
    }
  }

  object IntSchemaMetadataMapBinCodec extends Codec[Map[Int, SchemaMetadata]] {
    override def encode(value: Map[Int, SchemaMetadata]): Array[Byte] = {
      val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
      val oos = new ObjectOutputStream(stream)
      oos.writeObject(value.map(m => (m._1, SerializableSchemaMetadata.fromSchemaMetadata(m._2))))
      oos.close()
      stream.close()
      stream.toByteArray
    }

    override def decode(bytes: Array[Byte]): DecodingResult[Map[Int, SchemaMetadata]] = {
      val stream = new ByteArrayInputStream(bytes)
      val ois = new ObjectInputStream(stream)
      val value = ois.readObject
      ois.close()
      stream.close()

      val maybeValue = value.asInstanceOf[Map[Int, SerializableSchemaMetadata]]
      Codec.tryDecode(maybeValue.map(m => (m._1, m._2.getSchemaMetadata)))
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

  private val metadataCacheCodec: Codec[Map[Int, SchemaMetadata]] = new Codec[Map[Int, SchemaMetadata]] {

    def encode(value: Map[Int, SchemaMetadata]): Array[Byte] = IntSchemaMetadataMapBinCodec.encode(value)

    def decode(bytes: Array[Byte]): Codec.DecodingResult[Map[Int, SchemaMetadata]] = IntSchemaMetadataMapBinCodec.decode(bytes)
  }

  private def readIntConfigParameter(config: Config, path: String): Int = {
    config.getIntOpt(path)
      .getOrElse(throw new IllegalArgumentException(s"A $path is required."))
  }

  private def readStringConfigParameter(config: Config, path: String): String = {
    config.getStringOpt(path)
      .getOrElse(throw new IllegalArgumentException(s"A $path is required."))
  }

  private def readSslConfigParameter(config: Config, path: String): Boolean = {
    config.getBooleanOpt(path).getOrElse(true)
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
    val useSSL = readSslConfigParameter(config, "schema.registry.redis.ssl")

    val client = new RedisSchemaRegistryClient(
      baseUrl,
      redisHost,
      redisPort,
      schemaRegistrySecurityConfig.toConfigMap,
      CacheConfigs(redisIdCacheTtl, redisSchemaCacheTtl, redisVersionCacheTtl),
      useSSL
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
                                cacheConfigs: CacheConfigs,
                                ssl: Boolean) extends SchemaRegistryClient {

  def this(baseUrl: String, redisHost: String, redisPort: Int) {
    this(new RestService(baseUrl), redisHost, redisPort, Map.empty[String, String], Map.empty[String, Any], CacheConfigs(1, 1, 1), true)
  }

  def this(baseUrl: String, redisHost: String, redisPort: Int, ssl: Boolean) {
    this(new RestService(baseUrl), redisHost, redisPort, Map.empty[String, String], Map.empty[String, Any],  CacheConfigs(1, 1, 1), ssl)
  }

  def this(baseUrl: String, redisHost: String, redisPort: Int, cacheConfigs: CacheConfigs, ssl: Boolean) {
    this(new RestService(baseUrl), redisHost, redisPort, Map.empty[String, String], Map.empty[String, Any], cacheConfigs, ssl)
  }

  def this(baseUrl: String, redisHost: String, redisPort: Int, config: Map[String, Any], ssl: Boolean) {
    this(new RestService(baseUrl), redisHost, redisPort, Map.empty[String, String], config, CacheConfigs(1, 1, 1), ssl)
  }

  def this(baseUrl: String, redisHost: String, redisPort: Int, config: Map[String, Any], cacheConfigs: CacheConfigs, ssl: Boolean) {
    this(new RestService(baseUrl), redisHost, redisPort, Map.empty[String, String], config, cacheConfigs, ssl)
  }

  def this(baseUrl: String, httpHeaders: Map[String, String], redisHost: String, redisPort: Int, ssl: Boolean) {
    this(new RestService(baseUrl), redisHost, redisPort, httpHeaders, Map.empty[String, Any], CacheConfigs(1, 1, 1), ssl)
  }

  def this(baseUrl: String, httpHeaders: Map[String, String], redisHost: String, redisPort: Int, cacheConfigs: CacheConfigs, ssl: Boolean) {
    this(new RestService(baseUrl), redisHost, redisPort, httpHeaders, Map.empty[String, Any], cacheConfigs, ssl)
  }

  def this(baseUrl: String, redisHost: String, redisPort: Int, httpHeaders: Map[String, String], config: Map[String, Any], ssl: Boolean) {
    this(new RestService(baseUrl), redisHost, redisPort, httpHeaders, config, CacheConfigs(1, 1, 1), ssl)
  }

  def this(baseUrl: String, redisHost: String, redisPort: Int, httpHeaders: Map[String, String], config: Map[String, Any], cacheConfigs: CacheConfigs, ssl: Boolean) {
    this(new RestService(baseUrl), redisHost, redisPort, httpHeaders, config, cacheConfigs, ssl)
  }

  def this(baseUrls: List[String], redisHost: String, redisPort: Int, ssl: Boolean) {
    this(new RestService(baseUrls.asJava), redisHost, redisPort, Map.empty[String, String], Map.empty[String, Any], CacheConfigs(1, 1, 1), ssl)
  }

  def this(baseUrls: List[String], redisHost: String, redisPort: Int, cacheConfigs: CacheConfigs, ssl: Boolean) {
    this(new RestService(baseUrls.asJava), redisHost, redisPort, Map.empty[String, String], Map.empty[String, Any], cacheConfigs, ssl)
  }

  def this(baseUrls: List[String], redisHost: String, redisPort: Int, config: Map[String, Any], ssl: Boolean) {
    this(new RestService(baseUrls.asJava), redisHost, redisPort, Map.empty[String, String], config, CacheConfigs(1, 1, 1), ssl)
  }

  def this(baseUrls: List[String], redisHost: String, redisPort: Int, config: Map[String, Any], cacheConfigs: CacheConfigs, ssl: Boolean) {
    this(new RestService(baseUrls.asJava), redisHost, redisPort, Map.empty[String, String], config, cacheConfigs, ssl)
  }

  def this(baseUrls: List[String], httpHeaders: Map[String, String], redisHost: String, redisPort: Int, ssl: Boolean) {
    this(new RestService(baseUrls.asJava), redisHost, redisPort, httpHeaders, Map.empty[String, Any], CacheConfigs(1, 1, 1), ssl)
  }

  def this(baseUrls: List[String], httpHeaders: Map[String, String], redisHost: String, redisPort: Int, cacheConfigs: CacheConfigs, ssl: Boolean) {
    this(new RestService(baseUrls.asJava), redisHost, redisPort, httpHeaders, Map.empty[String, Any], cacheConfigs, ssl)
  }

  def this(baseUrls: List[String], redisHost: String, redisPort: Int, httpHeaders: Map[String, String], config: Map[String, Any], ssl: Boolean) {
    this(new RestService(baseUrls.asJava), redisHost, redisPort, httpHeaders, config, CacheConfigs(1, 1, 1), ssl)
  }

  def this(baseUrls: List[String], redisHost: String, redisPort: Int, httpHeaders: Map[String, String], config: Map[String, Any], cacheConfigs: CacheConfigs, ssl: Boolean) {
    this(new RestService(baseUrls.asJava), redisHost, redisPort, httpHeaders, config, cacheConfigs, ssl)
  }

  import hydra.avro.registry.RedisSchemaRegistryClient._

  private val idCacheConfig = CacheConfig.defaultCacheConfig
  private val schemaCacheConfig = CacheConfig.defaultCacheConfig
  private val versionCacheConfig = CacheConfig.defaultCacheConfig
  private val metadataCacheConfig = CacheConfig.defaultCacheConfig
  private val latestSchemaCacheConfig = CacheConfig.defaultCacheConfig

  private val idCacheDurationTtl = Option(Duration(cacheConfigs.idCacheTtl, TimeUnit.MINUTES))
  private val schemaCacheDurationTtl = Option(Duration(cacheConfigs.schemaCacheTtl, TimeUnit.MINUTES))
  private val versionCacheDurationTtl = Option(Duration(cacheConfigs.versionCacheTtl, TimeUnit.MINUTES))
  private val metadataCacheDurationTtl = Option(Duration(cacheConfigs.metadataCacheTtl.getOrElse(10000), TimeUnit.MINUTES))

  private def jedisFactory: JedisPool = {
    new JedisPool(redisHost, redisPort, ssl)
  }

  private val schemaCache: Cache[Map[Schema, Int]] =
    RedisCache(jedisFactory)(schemaCacheConfig, schemaCacheCodec)

  private val idCache: Cache[Map[Int, Schema]] =
    RedisCache(jedisFactory)(idCacheConfig, idCacheCodec)

  private val versionCache: Cache[Map[Schema, Int]] =
    RedisCache(jedisFactory)(versionCacheConfig, schemaCacheCodec)

  private val metadataCache: Cache[Map[Int, SchemaMetadata]] =
    RedisCache(jedisFactory)(metadataCacheConfig, metadataCacheCodec)

  private def buildSchemaKey(subject: String): String = {
    "schema_" + subject
  }

  private def buildIdKey(subject: String): String = {
    "id_" + subject
  }

  private def buildVersionKey(subject: String): String = {
    "version_" + subject
  }

  private def buildMetadataKey(subject: String): String = {
    "meta_" + subject
  }

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

  override def getBySubjectAndId(s: String, i: Int): Schema = {
    if (DO_NOT_CACHE_LIST.exists(s.startsWith)) {
      restGetSchemaById(s, i)
    } else {
      val idKey = buildIdKey(s)

      idCache.get(idKey) match {
        case Failure(_) => restGetId(s, i)(i)
        case Success(map) =>
          map match {
            case Some(m) if m.keys.exists(_ == i) => m(i)
            case _ => getBySubjectAndIdSynchronized(s, i)
          }
      }
    }
  }

  private def restGetSchemaById(s: String, i: Int): Schema = {
    val restSchema = restService.getId(i)
    val parser = new Schema.Parser()
    parser.setValidateDefaults(false)
    parser.parse(restSchema.getSchemaString)
  }

  private def restGetId(s: String, i: Int): Map[Int, Schema] = Try {
    val schema = restGetSchemaById(s, i)
    Map(i -> schema)
  }.getOrElse(Map.empty[Int, Schema])

  private def getBySubjectAndIdSynchronized(s: String, i: Int): Schema = synchronized {

    val idKey = buildIdKey(s)

    idCache.get(idKey) match {
      case Failure(_) =>
        restGetId(s, i)(i)
      case Success(map) =>
        map match {
          case Some(m) if m.keys.exists(_ == i) =>
            m(i)
          case Some(m) =>
            val c = restGetId(s, i)
            idCache.put(idKey)(m ++ c, idCacheDurationTtl)
            c(i)
          case None =>
            val c = restGetId(s, i)
            idCache.put(idKey)(c, idCacheDurationTtl)
            c(i)
        }
    }
  }

  override def getAllSubjectsById(i: Int): java.util.Collection[String] = {
    restService.getAllSubjectsById(i)
  }

   def getLatestSchemaMetadata(s: String): SchemaMetadata = synchronized {
    val response = restService.getLatestVersion(s)
    val id = response.getId
    val schema: String  = response.getSchema
    val version = response.getVersion
    new SchemaMetadata(id, version, schema)
  }

/*  override def getSchemaMetadata(s: String, i: Int): SchemaMetadata = {
    val response = restService.getVersion(s, i)
    val id = response.getId
    val schema: String = response.getSchema
    new SchemaMetadata(id, i, schema)
  }*/

  override def getSchemaMetadata(s: String, i: Int): SchemaMetadata = {
    def get(s: String, i: Int): SchemaMetadata = {
      val response = restService.getVersion(s, i)
      val id = response.getId
      val schema: String = response.getSchema
      new SchemaMetadata(id, i, schema)
    }

    if(DO_NOT_CACHE_LIST.exists(s.startsWith)) {
      get(s, i)
    } else {
      val metadataKey = buildMetadataKey(s)

      metadataCache.get(metadataKey) match {
        case Failure(_) =>
          val sm = get(s, i)
          metadataCache.put(metadataKey)(Map(i -> sm), metadataCacheDurationTtl)
          sm
        case Success(map) => map match {
          case Some(m) if m.keySet.contains(i) =>
            m(i)
          case Some(m) =>
            val sm = get(s, i)
            val concatMap: Map[Int, SchemaMetadata] = m ++ Map(i -> sm)
            metadataCache.put(metadataKey)(concatMap, metadataCacheDurationTtl)
            sm
          case None =>
            val sm = get(s, i)
            metadataCache.put(metadataKey)(Map(i -> sm), metadataCacheDurationTtl)
            sm
        }
      }
    }
  }

  override def getVersion(s: String, schema: Schema): Int = synchronized {
    if(DO_NOT_CACHE_LIST.exists(s.startsWith)) {
      val response: io.confluent.kafka.schemaregistry.client.rest.entities.Schema = restService.lookUpSubjectVersion(schema.toString(), s, true)
      response.getVersion.toInt
    } else {
      val versionKey = buildVersionKey(s)

      versionCache.get(versionKey) match {
        case Failure(_) =>
          val response: io.confluent.kafka.schemaregistry.client.rest.entities.Schema = restService.lookUpSubjectVersion(schema.toString(), s, true)
          versionCache.put(versionKey)(Map(schema -> response.getVersion.toInt), versionCacheDurationTtl)
          response.getVersion.toInt
        case Success(map) =>
          map match {
            case Some(m) if m.keySet.contains(schema) =>
              m(schema)
            case Some(m) =>
              val response: io.confluent.kafka.schemaregistry.client.rest.entities.Schema = restService.lookUpSubjectVersion(schema.toString(), s, true)
              val concatMap: Map[Schema, Int] = m ++ Map(schema -> response.getVersion.toInt)
              versionCache.put(versionKey)(concatMap, versionCacheDurationTtl)
              response.getVersion.toInt
            case None =>
              val response: io.confluent.kafka.schemaregistry.client.rest.entities.Schema = restService.lookUpSubjectVersion(schema.toString(), s, true)
              versionCache.put(versionKey)(Map(schema -> response.getVersion.toInt), versionCacheDurationTtl)
              response.getVersion.toInt
          }
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
    if (DO_NOT_CACHE_LIST.exists(s.startsWith)) {
      restService.lookUpSubjectVersion(schema.toString, s, false).getId.toInt
    } else {
      def call(): Map[Schema, Int] = {
        val response = restService.lookUpSubjectVersion(schema.toString, s, false)
        Map(schema -> response.getId.toInt)
      }

      val idKey = buildIdKey(s)

      def populateVersionCache(m: Map[Schema, Int]): Try[Any] = {
        val idM: Map[Int, Schema] = m.map(kv => kv._2 -> kv._1)

        idCache.get(idKey) match {
          case Failure(_) =>
            idCache.put(idKey)(idM, idCacheDurationTtl)
          case Success(im) =>
            val concatMap: Map[Int, Schema] = idM ++ im.getOrElse(Map.empty[Int, Schema])
            idCache.put(idKey)(concatMap, idCacheDurationTtl)
        }
      }

      val schemaKey = buildSchemaKey(s)

      schemaCache.get(schemaKey) match {
        case Failure(_) =>
          schemaCache.put(schemaKey)(call(), schemaCacheDurationTtl)
          call()(schema)
        case Success(map) => map match {
          case Some(m) if m.keys.exists(_ == schema) =>
            populateVersionCache(m)
            m(schema)
          case Some(m) =>
            val concatMaps: Map[Schema, Int] = m ++ call()
            populateVersionCache(concatMaps)
            schemaCache.put(schemaKey)(concatMaps, schemaCacheDurationTtl)
            call()(schema)
          case None =>
            schemaCache.put(schemaKey)(call(), schemaCacheDurationTtl)
            call()(schema)
        }
      }
    }
  }

  override def deleteSubject(s: String): java.util.List[Integer] = synchronized {
    deleteSubject(DEFAULT_REQUEST_PROPERTIES.asJava, s)
  }

  override def deleteSubject(map: java.util.Map[String, String], s: String): java.util.List[Integer] = synchronized {
    versionCache.remove(buildVersionKey(s))
    idCache.remove(buildIdKey(s))
    schemaCache.remove(buildSchemaKey(s))
    metadataCache.remove(buildMetadataKey(s))
    restService.deleteSubject(map, s)
  }

  override def deleteSchemaVersion(s: String, s1: String): Integer = {
    deleteSchemaVersion(DEFAULT_REQUEST_PROPERTIES.asJava, s, s1)
  }

  override def deleteSchemaVersion(map: java.util.Map[String, String], s: String, s1: String): Integer = synchronized {
    Try(restService.deleteSchemaVersion(map, s, s1)) match {
      case Failure(exception) => throw exception
      case Success(value) =>
        val versionKey = buildVersionKey(s)
        val cacheMap: Map[Schema, Int] = versionCache.get(versionKey).map(_.get).getOrElse(Map.empty[Schema, Int])

        if (cacheMap.nonEmpty) {
          if (cacheMap.values.exists(_ == value)) {
            versionCache.put(versionKey)(cacheMap.filterNot(kv => kv._2 == value), versionCacheDurationTtl)
          }
        }
        value
    }
  }

  override def register(s: String, schema: Schema, i: Int, i1: Int): Int = synchronized {
    def register(): Int =
      if (i >= 0) {
        restService.registerSchema(schema.toString(), s, i, i1)
      } else {
        restService.registerSchema(schema.toString(), s)
      }

    if(DO_NOT_CACHE_LIST.exists(s.startsWith)) {
      register()
    } else {
      val idKey = buildIdKey(s)

      def populateIdCache(sc: Schema, id: Int): Unit = {
        idCache.caching(idKey)(idCacheDurationTtl) {
          Map(id -> sc)
        } match {
          case Failure(_) => idCache.put(idKey)(Map(id -> sc), idCacheDurationTtl)
          case Success(m) if m.exists(_ == (id -> sc)) => ()
          case Success(m) =>
            val concatMap: Map[Int, Schema] = Map(id -> sc) ++ m
            idCache.put(idKey)(concatMap, idCacheDurationTtl)
        }
      }

      val schemaKey = buildSchemaKey(s)

      schemaCache.get(schemaKey) match {
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
            val concatMap: Map[Schema, Int] = Map(schema -> retrievedId) ++ m
            schemaCache.put(schemaKey)(concatMap, schemaCacheDurationTtl)
            populateIdCache(schema, retrievedId)
            retrievedId
          case None =>
            val retrievedId = register()
            populateIdCache(schema, retrievedId)
            retrievedId
        }
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

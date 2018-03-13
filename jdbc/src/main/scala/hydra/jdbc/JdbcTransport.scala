package hydra.jdbc

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory, ConfigObject}
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import hydra.avro.io.{SaveMode, Upsert}
import hydra.common.config.ConfigSupport
import hydra.core.transport.Transport
import hydra.core.transport.Transport.Deliver
import hydra.sql.{JdbcDialects, JdbcRecordWriter, TableIdentifier}
import configs.syntax._
import hydra.avro.util.SchemaWrapper
import hydra.common.logging.LoggingAdapter
import hydra.common.util.TryWith

import scala.collection.mutable
import scala.util.{Success, Try}
import scala.collection.JavaConverters._

class JdbcTransport extends Transport with ConfigSupport with LoggingAdapter {

  private[jdbc] val dbProfiles = new mutable.HashMap[String, DbProfile]()

  private val writers = new mutable.HashMap[String, JdbcRecordWriter]()

  override def transport = {
    case Deliver(record: JdbcRecord, deliveryId, callback) =>
      Try {
        val writer = getOrUpdateWriter(dbProfiles(record.dbProfile), record)
        writer.execute(Upsert(record.payload))
        callback.onCompletion(deliveryId, Some(JdbcRecordMetadata(record.destination)), None)
      }.recover {
        case e: Exception =>
          callback.onCompletion(deliveryId, None, Some(e))
      }
  }

  private def getOrUpdateWriter(db: DbProfile, rec: JdbcRecord) = {
    //TODO: Make the writer constructor params configurable. Should we support batching?
    val schema = rec.payload.getSchema
    val key = s"${db.name}|${schema.getFullName}"
    writers.getOrElseUpdate(key, new JdbcRecordWriter(db.ds,
      SchemaWrapper.from(schema, rec.key.getOrElse(Seq.empty)), SaveMode.Append,
      JdbcDialects.get(JdbcTransport.getUrl(db)), batchSize = -1,
      tableIdentifier = Some(TableIdentifier(rec.destination))))
  }

  override def preStart(): Unit = {
    writers.clear()
    applicationConfig.getOrElse[Config]("transports.jdbc.profiles", ConfigFactory.empty).map { cfg =>
      cfg.root().entrySet().asScala.foreach { e =>
        val props = new Properties
        props.putAll(ConfigSupport.toMap(e.getValue.asInstanceOf[ConfigObject].toConfig).asJava)
        dbProfiles.put(e.getKey, new DbProfile(e.getKey, props))
      }
    }

    log.debug(s"Available database profiles: ${dbProfiles.keySet.mkString(",")}")
  }

  override def postStop(): Unit = {
    writers.foreach(_._2.flush())
    dbProfiles.foreach(_._2.close())
  }
}

object JdbcTransport {
  /**
    * Retrives the connection URL, either from the jdbcUrl property config or by querying the connection metadata.
    *
    * @param db
    * @return
    */
  private[jdbc] def getUrl(db: DbProfile): String = {
    Option(db.ds.getJdbcUrl).map(Success(_)).getOrElse {
      TryWith(db.ds.getConnection)(_.getMetaData.getURL)
    }.get
  }
}


class DbProfile(val name: String, props: Properties) {

  private val hcfg = new HikariConfig(props)

  lazy val ds = new HikariDataSource(hcfg)

  def close() = ds.close()
}
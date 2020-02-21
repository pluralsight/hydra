package hydra.jdbc

import akka.actor.ActorRef
import akka.pattern.ask
import akka.util
import com.pluralsight.hydra.avro.JsonConverter
import hydra.avro.resource.SchemaResource
import hydra.avro.util.{AvroUtils, SchemaWrapper}
import hydra.common.config.ConfigSupport
import hydra.core.akka.SchemaRegistryActor.{
  FetchSchemaRequest,
  FetchSchemaResponse
}
import hydra.core.ingest.HydraRequest
import hydra.core.ingest.RequestParams.HYDRA_SCHEMA_PARAM
import hydra.core.protocol.MissingMetadataException
import hydra.core.transport.ValidationStrategy.Strict
import hydra.core.transport.{
  AckStrategy,
  HydraRecord,
  RecordFactory,
  RecordMetadata
}
import hydra.jdbc.JdbcRecordFactory.{DB_PROFILE_PARAM, TABLE_PARAM}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class JdbcRecordFactory(schemaResourceLoader: ActorRef)
    extends RecordFactory[Seq[String], GenericRecord]
    with ConfigSupport {

  private implicit val timeout = util.Timeout(3.seconds)

  override def build(
      request: HydraRequest
  )(implicit ec: ExecutionContext): Future[JdbcRecord] = {
    for {
      subject <- Future.fromTry(JdbcRecordFactory.getSchemaName(request))
      schema <- (schemaResourceLoader ? FetchSchemaRequest(subject))
        .mapTo[FetchSchemaResponse]
        .map(_.schemaResource)
      avro <- convert(schema, request)
      record <- buildRecord(request, avro, schema.schema, request.ackStrategy)
    } yield record

  }

  private def convert(schemaResource: SchemaResource, request: HydraRequest)(
      implicit ec: ExecutionContext
  ): Future[GenericRecord] = {
    val converter = new JsonConverter[GenericRecord](
      schemaResource.schema,
      request.validationStrategy == Strict
    )
    Future(converter.convert(request.payload))
      .recover {
        case ex => throw AvroUtils.improveException(ex, schemaResource)
      }
  }

  private def buildRecord(
      request: HydraRequest,
      record: GenericRecord,
      schema: Schema,
      ackStrategy: AckStrategy
  )(implicit ec: ExecutionContext): Future[JdbcRecord] = {
    Future {
      val table = request.metadataValue(TABLE_PARAM).getOrElse(schema.getName)

      val dbProfile = request
        .metadataValue(DB_PROFILE_PARAM)
        .getOrElse(
          throw MissingMetadataException(
            DB_PROFILE_PARAM,
            s"A db profile name is required ${DB_PROFILE_PARAM}]."
          )
        )

      JdbcRecord(
        table,
        JdbcRecordFactory.pk(request, schema),
        record,
        dbProfile,
        ackStrategy
      )
    }
  }
}

object JdbcRecordFactory {
  val PRIMARY_KEY_PARAM = "hydra-db-primary-key"

  val TABLE_PARAM = "hydra-dtable"

  val DB_PROFILE_PARAM = "hydra-db-profile"

  private[jdbc] def pk(request: HydraRequest, schema: Schema): Seq[String] = {
    request.metadataValue(PRIMARY_KEY_PARAM).map(_.split(",")) match {
      case Some(ids) => ids
      case None      => SchemaWrapper.from(schema).primaryKeys //todo: cache this?
    }
  }

  private[jdbc] def getSchemaName(request: HydraRequest): Try[String] = {
    request
      .metadataValue(HYDRA_SCHEMA_PARAM)
      .map(Success(_))
      .getOrElse(
        Failure(
          MissingMetadataException(
            HYDRA_SCHEMA_PARAM,
            s"A schema name is required [${HYDRA_SCHEMA_PARAM}]."
          )
        )
      )
  }
}

case class JdbcRecord(
    destination: String,
    key: Seq[String],
    payload: GenericRecord,
    dbProfile: String,
    ackStrategy: AckStrategy
) extends HydraRecord[Seq[String], GenericRecord] {
  lazy val primaryKeys = key

  lazy val keyValues: Map[String, AnyRef] =
    primaryKeys.map(k => k -> payload.get(k)).toMap
}

case class JdbcRecordMetadata(
    destination: String,
    timestamp: Long = System.currentTimeMillis,
    ackStrategy: AckStrategy
) extends RecordMetadata

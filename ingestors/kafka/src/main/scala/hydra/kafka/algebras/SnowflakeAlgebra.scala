package hydra.kafka.algebras

import hydra.avro.convert.SimpleStringToGenericRecord
import hydra.kafka.algebras.KafkaClientAlgebra.PublishResponse
import SimpleStringToGenericRecord._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import hydra.kafka.model.Schemas
import vulcan.Codec
import vulcan.generic._
import cats.data.Validated
import cats.data.Validated.{Invalid, Valid}
import cats.effect.concurrent.Ref
import cats.effect.{Concurrent, Sync}
import cats.implicits._
import io.chrisdavenport.log4cats.Logger
import org.apache.avro.generic.GenericRecord
import spray.json._


sealed abstract class TableType extends Product with Serializable {
  def asString: String = this match {
    case TableType.Current => "current"
    case TableType.Historical => "historical"
  }
}
object TableType {
  case object Current extends TableType
  case object Historical extends TableType
}

trait SnowflakeAlgebra[F[_]] {
  getCurrentTableNames: F[List[String]]
  getHistoricalTableNames: F[List[String]]
  getAllTableNames: F[Map[TableType, List[String]]]
  getTableNamesForTopic: F[Map[TableType, String]]
}

object SnowflakeAlgebra {
  def make[F[_]: Sync: Concurrent: Logger](): F[SnowflakeAlgebra[F]] = {
    val snowflakeConnectionUrl =
    val connectionURL = "jdbc:snowflake://KAA65375.snowflakecomputing.com/?user=READONLY_SERVICE_USER&password=5Vzi6D$EcckJzP^e4Tqi&warehouse=READONLY&role=READONLY"
    for {
      ref <- Ref[F].of(SnowflakeStorageFacade.empty)

    } yield algebra

    /*val tagsStream: fs2.Stream[F,(GenericRecord,Option[GenericRecord])] =
      kafkaClientAlgebra.consumeMessages(tagsTopic, tagsClient, commitOffsets = false).map(record => (record._1, record._2))
    for {
      ref <- Ref[F].of(TagsStorageFacade.empty)
      _ <- Concurrent[F].start(tagsStream.flatMap{
        case (key, value) =>
          fs2.Stream.eval {
            value match {
              case Some(value) =>
                ref.update(_.addMetadata(HydraTag(key.get("name").toString, value.get("description").toString)))
              case None =>
                ref.update(_.removeMetadata(key.toString))
            }
          }
        case e =>
          fs2.Stream.eval(Logger[F].error(s"Unexpected return from Kafka: ${e.toString()}"))
      }.recoverWith {
        case e =>
          fs2.Stream.eval(Logger[F].warn(s"Error in TagsAlgebra"))
      }.compile.drain)
      algebra <- getTagsAlgebra(ref, tagsTopic, kafkaClientAlgebra)
    } yield algebra*/
  }
}

private case class SnowflakeStorageFacade(tablesMap: Map[TableType, List[String]]) {
  def updateTableNamesList(tableType: TableType, tableNames: List[String]): SnowflakeStorageFacade = {
    this.copy(this.tablesMap + (tableType -> tableNames))
  }
}

private object SnowflakeStorageFacade {
  def empty: SnowflakeStorageFacade = SnowflakeStorageFacade(Map(TableType.Current -> List[String](), TableType.Historical -> List[String]()))
}
package hydra.avro.registry

import cats.effect.Sync
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, MockSchemaRegistryClient, SchemaRegistryClient}
import org.apache.avro.Schema

trait SchemaRegistry[F[_]] {

  import SchemaRegistry._

  def registerSchema(subject: String, schema: Schema): F[SchemaId]

  def deleteSchemaOfVersion(subject: String, version: SchemaVersion): F[Unit]

  def getVersion(subject: String, schema: Schema): F[SchemaVersion]

  def getAllVersions(subject: String): F[List[SchemaVersion]]

  def getAllSubjects: F[List[String]]

}

object SchemaRegistry {

  type SchemaId = Int
  type SchemaVersion = Int

  def live[F[_]: Sync](schemaRegistryBaseUrl: String, maxCacheSize: Int): F[SchemaRegistry[F]] = Sync[F].delay {
    getFromSchemaRegistryClient(new CachedSchemaRegistryClient(schemaRegistryBaseUrl, maxCacheSize))
  }

  def test[F[_]: Sync]: F[SchemaRegistry[F]] = Sync[F].delay {
    getFromSchemaRegistryClient(new MockSchemaRegistryClient)
  }

  private[this] def getFromSchemaRegistryClient[F[_]: Sync](schemaRegistryClient: SchemaRegistryClient): SchemaRegistry[F] =
    new SchemaRegistry[F] {

      override def registerSchema(subject: String, schema: Schema): F[SchemaId] =
        Sync[F].delay(schemaRegistryClient.register(subject, schema))

      override def deleteSchemaOfVersion(subject: String, version: SchemaVersion): F[Unit] =
        Sync[F].delay(schemaRegistryClient.deleteSchemaVersion(subject, version.toString))

      override def getVersion(subject: String, schema: Schema): F[SchemaVersion] =
        Sync[F].delay {
          schemaRegistryClient.getVersion(subject, schema)
        }

      override def getAllVersions(subject: String): F[List[SchemaId]] =
        Sync[F].delay {
          import collection.JavaConverters._
          schemaRegistryClient.getAllVersions(subject)
            .asScala
            .toList
            .map(_.toInt)
        }

        override def getAllSubjects: F[List[String]] =
          Sync[F].delay {
           import collection.JavaConverters._
           schemaRegistryClient.getAllSubjects
           .asScala
           .toList 
          }

  }

}

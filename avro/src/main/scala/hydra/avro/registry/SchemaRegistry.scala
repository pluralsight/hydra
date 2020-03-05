package hydra.avro.registry

import cats.effect.Sync
import io.confluent.kafka.schemaregistry.client.{
  CachedSchemaRegistryClient,
  MockSchemaRegistryClient,
  SchemaRegistryClient
}
import org.apache.avro.Schema

/**
  * Internal interface to interact with the SchemaRegistryClient from Confluent.
  * Abstraction allows pure functional interface for working with underlying Java implementation.
  * Provides a live version for production usage and a test version for integration testing.
  * @tparam F - higher kinded type - polymorphic effect type
  */
trait SchemaRegistry[F[_]] {

  import SchemaRegistry._

  /**
    * Adds schema to the configured SchemaRegistry. Registration is idempotent.
    * Equivalency is determined by taking a hash of the given schema. Any changes to the schema change the hash.
    * @param subject - subject name for the schema found in SchemaRegistry including the suffix (-key | -value)
    * @param schema - avro Schema which is to be added to the Schema Registry
    * @return SchemaId for schema, whether newly created or preexisting
    */
  def registerSchema(subject: String, schema: Schema): F[SchemaId]

  /**
    * Deletes schema from the configured SchemaRegistry. Deletes only the version specified and only one of the
    * key /value, whichever was specified in the subject suffix.
    * @param subject - subject name for the schema found in SchemaRegistry including the suffix (-key | -value)
    * @param version - integer representing the schema version
    * @return Unit
    */
  def deleteSchemaOfVersion(subject: String, version: SchemaVersion): F[Unit]

  /**
    * Retrieves the SchemaVersion if the given subject and schema match an item in SchemaRegistry.
    * The schema hash must exactly match one of the schemas stored in Schema Registry. All fields must be equal.
    * If the schema is not found, the error will be reported in the error channel of the higher kinded type (F[_]).
    * @param subject - subject name for the schema found in SchemaRegistry including the suffix (-key | -value)
    * @param schema - avro Schema which is expected to be in Schema Registry
    * @return SchemaVersion
    */
  def getVersion(subject: String, schema: Schema): F[SchemaVersion]

  /**
    * Retrieves all SchemaVersion(s) for a given subject.
    * @param subject - subject name for the schema found in SchemaRegistry including the suffix (-key | -value)
    * @return List[SchemaVersion]
    */
  def getAllVersions(subject: String): F[List[SchemaVersion]]

  /**
    * Retrieves all subjects found in the SchemaRegistry
    * @return List[String]
    */
  def getAllSubjects: F[List[String]]

}

object SchemaRegistry {

  type SchemaId = Int
  type SchemaVersion = Int

  def live[F[_]: Sync](
      schemaRegistryBaseUrl: String,
      maxCacheSize: Int
  ): F[SchemaRegistry[F]] = Sync[F].delay {
    getFromSchemaRegistryClient(
      new CachedSchemaRegistryClient(schemaRegistryBaseUrl, maxCacheSize)
    )
  }

  def test[F[_]: Sync]: F[SchemaRegistry[F]] = Sync[F].delay {
    getFromSchemaRegistryClient(new MockSchemaRegistryClient)
  }

  private[this] def getFromSchemaRegistryClient[F[_]: Sync](
      schemaRegistryClient: SchemaRegistryClient
  ): SchemaRegistry[F] =
    new SchemaRegistry[F] {

      override def registerSchema(
          subject: String,
          schema: Schema
      ): F[SchemaId] =
        Sync[F].delay(schemaRegistryClient.register(subject, schema))

      override def deleteSchemaOfVersion(
          subject: String,
          version: SchemaVersion
      ): F[Unit] =
        Sync[F].delay(
          schemaRegistryClient.deleteSchemaVersion(subject, version.toString)
        )

      override def getVersion(
          subject: String,
          schema: Schema
      ): F[SchemaVersion] =
        Sync[F].delay {
          schemaRegistryClient.getVersion(subject, schema)
        }

      override def getAllVersions(subject: String): F[List[SchemaId]] =
        Sync[F].delay {
          import collection.JavaConverters._
          schemaRegistryClient
            .getAllVersions(subject)
            .asScala
            .toList
            .map(_.toInt)
        }

      override def getAllSubjects: F[List[String]] =
        Sync[F].delay {
          import collection.JavaConverters._
          schemaRegistryClient.getAllSubjects.asScala.toList
        }

    }

}

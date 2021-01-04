package hydra.avro.registry

import cats.effect.Sync
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, MockSchemaRegistryClient, SchemaRegistryClient}
import org.apache.avro.{Schema, SchemaValidatorBuilder}
import cats.syntax.all._
import io.confluent.kafka.schemaregistry.avro.AvroCompatibilityChecker
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException
import javax.security.auth.Subject

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

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
    * Deletes the subject from the versionCache, idCache, and schemaCache
    * of the CachedSchemaRegistryClient
    * @param subject The subject using -key or -value to delete
    * @return Unit
    */
  def deleteSchemaSubject(subject: String): F[Unit]

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
    * @return List[SchemaVersion] or List.empty if Subject Not Found
    */
  def getAllVersions(subject: String): F[List[SchemaVersion]]

  /**
    * Retrieves all subjects found in the SchemaRegistry
    * @return List[String]
    */
  def getAllSubjects: F[List[String]]

  /**
    * Retrieves the SchemaRegistryClient from the algebra
    * @return SchemaRegistryClient
    */
  def getSchemaRegistryClient: F[SchemaRegistryClient]

  /**
    * Retrieves the latest schema for the given subject name, if exists
    * @param subject - subject name for the schema found in SchemaRegistry including the suffix (-key | -value)
    * @return - Optional Schema for the given subject name
    */
  def getLatestSchemaBySubject(subject: String): F[Option[Schema]]

  /**
    * Retrieves schema for the version and subject specified, if exists
    * @param subject - subject name for the schema found in SchemaRegistry including the suffix (-key | -value)
    * @param schemaVersion - version number for the schema
    * @return Optional Schema for the given subject and version combination
    */
  def getSchemaFor(subject: String, schemaVersion: SchemaVersion): F[Option[Schema]]

}

object SchemaRegistry {

  private[registry] implicit class CheckKeySchemaEvolution[F[_]: Sync](schemasF: F[List[Schema]]) {
    def checkKeyEvolution(subject: String, newSchema: Schema): F[List[Schema]] = schemasF.flatTap[Unit] {
      case _ if subject.endsWith("-value") => Sync[F].unit
      case Nil => Sync[F].unit
      case oldSchemas =>
        if (oldSchemas.forall(_.hashCode == newSchema.hashCode)) {
          Sync[F].unit
        } else {
          Sync[F].raiseError(IncompatibleSchemaException(s"Key schema evolutions are not permitted unless to add inconsequential elements i.e. doc fields."))
        }
    }
  }

  final case class IncompatibleSchemaException(message: String) extends
    RuntimeException(message)

  type SchemaId = Int
  type SchemaVersion = Int


  private[registry] def validate(newSchema: Schema, oldSchemas: List[Schema]): Boolean = {
    AvroCompatibilityChecker.FULL_TRANSITIVE_CHECKER.isCompatible(newSchema, oldSchemas.asJava)
  }

  def live[F[_]: Sync](
      schemaRegistryBaseUrl: String,
      maxCacheSize: Int
  ): F[SchemaRegistry[F]] = Sync[F].delay {
    getFromSchemaRegistryClient(new CachedSchemaRegistryClient(schemaRegistryBaseUrl, maxCacheSize))
  }

  def test[F[_]: Sync]: F[SchemaRegistry[F]] = Sync[F].delay {
    getFromSchemaRegistryClient(new MockSchemaRegistryClient)
  }

  private def getFromSchemaRegistryClient[F[_]: Sync](schemaRegistryClient: SchemaRegistryClient): SchemaRegistry[F] =
    new SchemaRegistry[F] {

      override def registerSchema(
          subject: String,
          schema: Schema
      ): F[SchemaId] = {
        for {
          versions <- getAllVersions(subject)
          schemas <- versions.traverse(getSchemaFor(subject, _)).map(_.flatten).checkKeyEvolution(subject, schema)
          validated <- Sync[F].delay(validate(schema, schemas.reverse))
          schemaVersion <- if (validated) {
            Sync[F].delay(schemaRegistryClient.register(subject, schema))
          } else {
            Sync[F].raiseError[SchemaVersion](IncompatibleSchemaException("Incompatible Schema Evolution. You may add fields with default fields, or remove fields with default fields."))
          }
        } yield schemaVersion
      }

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
        Sync[F].fromTry(Try(schemaRegistryClient.getAllVersions(subject)))
          .map(_.asScala.toList.map(_.toInt)).recover {
          case r: RestClientException if r.getErrorCode == 40401 => List.empty
        }

      override def getAllSubjects: F[List[String]] =
        Sync[F].delay {
          import collection.JavaConverters._
          schemaRegistryClient.getAllSubjects.asScala.toList
        }

      override def getSchemaRegistryClient: F[SchemaRegistryClient] = Sync[F].pure(schemaRegistryClient)

      //TODO: Test this
      override def getLatestSchemaBySubject(subject: String): F[Option[Schema]] = Sync[F].delay {
        Try {
          new org.apache.avro.Schema.Parser().parse(schemaRegistryClient.getLatestSchemaMetadata(subject).getSchema)
        }.toOption
      }

      override def getSchemaFor(subject: String, schemaVersion: SchemaVersion): F[Option[Schema]] = Sync[F].delay {
        Try {
          new org.apache.avro.Schema.Parser().parse(schemaRegistryClient.getSchemaMetadata(subject, schemaVersion).getSchema)
        }.toOption
      }

      override def deleteSchemaSubject(subject: String): F[Unit] =
        Sync[F].delay {
          schemaRegistryClient.deleteSubject(subject)
        }
    }

}

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
import org.apache.avro.LogicalTypes
import cats.kernel.Monoid
import cats.Foldable
import cats.Eval
import org.apache.avro.LogicalType

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

  final case class LogicalTypeBaseTypeMismatch(baseType: Schema.Type, logicalType: LogicalType, fieldName: String)
  final case class LogicalTypeBaseTypeMismatchErrors(errors: List[LogicalTypeBaseTypeMismatch]) extends
    RuntimeException(
      errors.map(e => s"Field named '${e.fieldName}' contains mismatch in " +
        s"baseType of '${e.baseType.getName}' and logicalType of '${e.logicalType.getName}'").mkString
    )

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

      private implicit class SchemaOps(sch: Schema) {
        def fields: List[Schema.Field] = sch.getType match {
          case Schema.Type.RECORD => sch.getFields.asScala.toList
          case Schema.Type.UNION => sch.getTypes.asScala.toList.flatMap(_.fields)
          case Schema.Type.MAP => sch.getValueType.fields
          case Schema.Type.ARRAY => sch.getElementType.fields
          case _ => List.empty
        }
      }

      private def foldMapAll[A: Monoid](start: Schema.Field)(f: Schema.Field => A): A = {
        val isThisLayerValid = f(start)
        val areOtherLayersValid = start.schema.fields.foldMap(foldMapAll[A](_)(f))
        Monoid[A].combine(isThisLayerValid, areOtherLayersValid)
      }

      private def checkLogicalTypesCompat(sch: Schema): F[Unit] = {
        val Uuid = LogicalTypes.uuid
        val TimestampMillis = LogicalTypes.timestampMillis
        val errors = sch.fields.foldMap(foldMapAll(_) { field =>
          val s = field.schema
          def checkTypesMatch(expected: Schema.Type, logicalType: LogicalType): List[LogicalTypeBaseTypeMismatch] = {
            if (s.getType == expected) {
               List.empty
            } else {
              List(LogicalTypeBaseTypeMismatch(s.getType, logicalType, field.name))
            }
          }
          Option(s.getLogicalType) match {
            case Some(TimestampMillis) => checkTypesMatch(Schema.Type.LONG, TimestampMillis)
            case Some(Uuid) => checkTypesMatch(Schema.Type.STRING, Uuid)
            case _ => List.empty
          }
        })
        errors match {
          case Nil => Sync[F].unit
          case errs => Sync[F].raiseError(LogicalTypeBaseTypeMismatchErrors(errs))
        }
      }

      override def registerSchema(
          subject: String,
          schema: Schema
      ): F[SchemaId] = {
        for {
          versions <- getAllVersions(subject)
          schemas <- versions.traverse(getSchemaFor(subject, _)).map(_.flatten).checkKeyEvolution(subject, schema)
          validated <- Sync[F].pure(validate(schema, schemas.reverse))
          _ <- checkLogicalTypesCompat(schema)
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

package hydra.kafka.programs
import java.time.Instant
import cats.effect.{Bracket, ExitCase, Resource, Sync}
import hydra.avro.registry.SchemaRegistry
import hydra.avro.registry.SchemaRegistry.{IllegalLogicalTypeChange, IllegalLogicalTypeChangeErrors, IncompatibleSchemaException, SchemaVersion}
import hydra.kafka.algebras.{KafkaAdminAlgebra, KafkaClientAlgebra, MetadataAlgebra}
import hydra.kafka.model.TopicMetadataV2Request.Subject
import hydra.kafka.model.{Schemas, StreamTypeV2, TopicMetadataV2, TopicMetadataV2Key, TopicMetadataV2Request}
import hydra.kafka.programs.CreateTopicProgram.{IncompatibleKeyAndValueFieldNames, KeyAndValueMismatch, KeyAndValueNotRecordType, KeyHasNullableFields, NullableField, NullableFieldWithoutDefaultValue, NullableFieldsNeedDefaultValue, UnsupportedLogicalType, ValidationErrors, getLogicalType}
import hydra.kafka.util.KafkaUtils.TopicDetails
import io.chrisdavenport.log4cats.Logger
import org.apache.avro.{LogicalType, Schema, SchemaBuilder}
import retry.syntax.all._
import retry.{RetryDetails, RetryPolicy, _}
import cats.implicits._
import hydra.avro.convert.IsoDate

import scala.jdk.CollectionConverters.collectionAsScalaIterableConverter

final class CreateTopicProgram[F[_]: Bracket[*[_], Throwable]: Sleep: Logger](
                                                                               schemaRegistry: SchemaRegistry[F],
                                                                               kafkaAdmin: KafkaAdminAlgebra[F],
                                                                               kafkaClient: KafkaClientAlgebra[F],
                                                                               retryPolicy: RetryPolicy[F],
                                                                               v2MetadataTopicName: Subject,
                                                                               metadataAlgebra: MetadataAlgebra[F]
                                                                             ) {

  private def onFailure(resourceTried: String): (Throwable, RetryDetails) => F[Unit] = {
    (error, retryDetails) =>
      Logger[F].info(
        s"Retrying due to failure in $resourceTried: $error. RetryDetails: $retryDetails"
      )
  }

  private def registerSchema(
                              subject: Subject,
                              schema: Schema,
                              isKey: Boolean
                            ): Resource[F, Unit] = {
    val suffixedSubject = subject.value + (if (isKey) "-key" else "-value")
    val registerSchema: F[Option[SchemaVersion]] = {
      schemaRegistry
        .getVersion(suffixedSubject, schema)
        .attempt
        .map(_.toOption)
        .flatMap { previousSchemaVersion =>
          schemaRegistry.registerSchema(suffixedSubject, schema) *>
            schemaRegistry.getVersion(suffixedSubject, schema).map {
              newSchemaVersion =>
                if (previousSchemaVersion.contains(newSchemaVersion)) None
                else Some(newSchemaVersion)
            }
        }
    }.retryingOnAllErrors(retryPolicy, onFailure("RegisterSchema"))
    Resource
      .makeCase(registerSchema)((newVersionMaybe, exitCase) =>
        (exitCase, newVersionMaybe) match {
          case (ExitCase.Error(_), Some(newVersion)) =>
            schemaRegistry.deleteSchemaOfVersion(suffixedSubject, newVersion)
          case _ => Bracket[F, Throwable].unit
        }
      )
      .void
  }

  private[programs] def registerSchemas(
                                         subject: Subject,
                                         keySchema: Schema,
                                         valueSchema: Schema
                                       ): Resource[F, Unit] = {
    registerSchema(subject, keySchema, isKey = true) *> registerSchema(
      subject,
      valueSchema,
      isKey = false
    )
  }

  private[programs] def createTopicResource(
                                             subject: Subject,
                                             topicDetails: TopicDetails
                                           ): Resource[F, Unit] = {
    val createTopic: F[Option[Subject]] =
      kafkaAdmin.describeTopic(subject.value).flatMap {
        case Some(_) => Bracket[F, Throwable].pure(None)
        case None =>
          kafkaAdmin
            .createTopic(subject.value, topicDetails)
            .retryingOnAllErrors(retryPolicy, onFailure("CreateTopicResource")) *> Bracket[
            F,
            Throwable
          ].pure(Some(subject))
      }
    Resource
      .makeCase(createTopic)({
        case (Some(_), ExitCase.Error(_)) =>
          kafkaAdmin.deleteTopic(subject.value)
        case _ => Bracket[F, Throwable].unit
      })
      .void
  }

  private def publishMetadata(
                               topicName: Subject,
                               createTopicRequest: TopicMetadataV2Request,
                             ): F[Unit] = {
    for {
      metadata <- metadataAlgebra.getMetadataFor(topicName)
      createdDate = metadata.map(_.value.createdDate).getOrElse(createTopicRequest.createdDate)
      deprecatedDate = metadata.map(_.value.deprecatedDate).getOrElse(createTopicRequest.deprecatedDate) match {
        case Some(date) =>
          Some(date)
        case None =>
          if(createTopicRequest.deprecated) {
            Some(Instant.now)
          } else {
            None
          }
      }
      message = (TopicMetadataV2Key(topicName), createTopicRequest.copy(createdDate = createdDate, deprecatedDate = deprecatedDate).toValue)
      records <- TopicMetadataV2.encode[F](message._1, Some(message._2), None)
      _ <- kafkaClient
        .publishMessage(records, v2MetadataTopicName.value)
        .rethrow
    } yield ()
  }

  private def validateKeySchemaEvolution(schemas: Schemas, subject: Subject): F[Option[IllegalLogicalTypeChangeErrors]] = {
    for {
      sch <- schemaRegistry.getLatestSchemaBySubject(subject + "-key")
    } yield {
      sch match {
        case Some(keySchema) =>
          val keyTransformationErrors = checkForIllegalLogicalTypeEvolutions(keySchema, schemas.key, keySchema.getName)
          if (keyTransformationErrors.nonEmpty) {
            IllegalLogicalTypeChangeErrors(keyTransformationErrors).some
          } else None
        case None => None
      }
    }
  }

  private def validateValueSchemaEvolution(schemas: Schemas, subject: Subject): F[Option[IllegalLogicalTypeChangeErrors]] = {
    for {
      sch <- schemaRegistry.getLatestSchemaBySubject(subject + "-value")
    } yield {
      sch match {
        case Some(valueSchema) =>
          val valueTransformationErrors = checkForIllegalLogicalTypeEvolutions(valueSchema, schemas.value, valueSchema.getName)
          if (valueTransformationErrors.nonEmpty){
            IllegalLogicalTypeChangeErrors(valueTransformationErrors).some
          } else None
        case None => None
      }
    }
  }

  private def checkForUnsupportedLogicalType(fields: List[Schema.Field]): Option[UnsupportedLogicalType] = {
    val fieldsWithUnsupportedTypes = fields.filter(getLogicalType(_).contains(IsoDate.IsoDateLogicalTypeName))
    Option(fieldsWithUnsupportedTypes).filter(_.nonEmpty).map(UnsupportedLogicalType)
  }

  private def checkForIllegalLogicalTypeEvolutions(existingSchema: Schema, newSchema: Schema, name: String) : List[IllegalLogicalTypeChange] = existingSchema.getType match {
    case Schema.Type.RECORD =>
      newSchema.getType match {
        case Schema.Type.RECORD =>
          existingSchema.getFields.asScala.toList.flatMap{ existingField =>
            newSchema.getFields.asScala.toList.filter(f => f.name() == existingField.name()).flatMap { newField =>
              checkForIllegalLogicalTypeEvolutions(existingField.schema(), newField.schema(), existingField.name())
            }
          }
        case _ => List()
      }
    case Schema.Type.ARRAY =>
      newSchema.getType match {
        case Schema.Type.ARRAY =>
          checkForIllegalLogicalTypeEvolutions(existingSchema.getElementType, newSchema.getElementType, existingSchema.getName)
        case _ => List()
      }
    case Schema.Type.MAP =>
      newSchema.getType match {
        case Schema.Type.MAP => {
          checkForIllegalLogicalTypeEvolutions(existingSchema.getValueType, newSchema.getValueType, existingSchema.getName)
        }
        case _ => List()
      }
    case Schema.Type.ENUM | Schema.Type.FIXED => {
      List()
    }
    case _ if existingSchema.isUnion => {
      if(newSchema.isUnion) existingSchema.getTypes.asScala.toList zip newSchema.getTypes.asScala.toList flatMap { t =>
        checkForIllegalLogicalTypeEvolutions(t._1, t._2, existingSchema.getName)
      } else List()
    }
    case _ =>
      if (existingSchema.getLogicalType != newSchema.getLogicalType)
        List(IllegalLogicalTypeChange(existingSchema.getLogicalType, newSchema.getLogicalType, name))
      else List()
  }

  private def checkForNullableKeyFields(keyFields: List[Schema.Field]): Option[KeyHasNullableFields] = {
    val nullableKeyFields = keyFields.flatMap(field => field.schema().getType match {
      case Schema.Type.UNION =>
        if (field.schema.getTypes.asScala.toList.exists(_.isNullable)) Some(NullableField(field.name(), field.schema())) else None
      case Schema.Type.NULL => Some(NullableField(field.name(), field.schema()))
      case _ => None
    })
    if (nullableKeyFields.nonEmpty) {
      KeyHasNullableFields(nullableKeyFields).some
    } else None
  }

  private def checkForDefaultNullableValueFields(valueFields: List[Schema.Field]): Option[NullableFieldsNeedDefaultValue] = {
    val errors = valueFields.flatMap { field => field.schema().getType match {
        case Schema.Type.UNION => if (field.schema().getTypes.asScala.toList.exists(_.isNullable) && Option(field.defaultVal()).isEmpty)
                                    Some(NullableFieldWithoutDefaultValue(field.name(), field.schema()))
                                  else None
        case _ => None
      }
    }

    if (errors.nonEmpty) {
      NullableFieldsNeedDefaultValue(errors).some
    } else  None
  }

  private def checkForMismatches(keyFields: List[Schema.Field], valueFields: List[Schema.Field]): Option[IncompatibleKeyAndValueFieldNames] = {
    val mismatches = keyFields.flatMap { k =>
      valueFields.flatMap { v =>
        if (k.name() == v.name() && !k.schema().equals(v.schema())) {
          Some(KeyAndValueMismatch(k.name(), k.schema(), v.schema()))
        } else {
          None
        }
      }
    }
    if (mismatches.nonEmpty) {
      IncompatibleKeyAndValueFieldNames(mismatches).some
    } else None
  }

  private def validateSchemaEvolutions(schemas: Schemas, subject: Subject): F[(Option[IllegalLogicalTypeChangeErrors], Option[IllegalLogicalTypeChangeErrors])] =
    for {
      k <- validateKeySchemaEvolution(schemas, subject)
      v <- validateValueSchemaEvolution(schemas, subject)
    } yield (k,v)

  private def validateKeyAndValueSchemasForStringRecordTypes(schemas: Schemas, subject: Subject, isKSQLTopic: Boolean): F[Unit] = {
    if (isKSQLTopic) {
      val concoctedKeyFields = SchemaBuilder
        .record("uselessRecord") //This is a useless record whose only purpose is to transform the string key into a list of fields.
        .fields()
        .name(schemas.key.getName)
        .`type`()
        .stringType()
        .noDefault()
        .endRecord().getFields.asScala.toList
      val valueFields = schemas.value.getFields.asScala.toList
      val validationErrors = for {
        kv <- validateSchemaEvolutions(schemas, subject)
      } yield {
        val valueNullableFieldCheckedForDefault = checkForDefaultNullableValueFields(valueFields)
        val keyFieldsCheckedUnsupportedLogicalType = checkForUnsupportedLogicalType(concoctedKeyFields)
        val valueFieldsCheckedUnsupportedLogicalType =  checkForUnsupportedLogicalType(valueFields)
        List[Option[RuntimeException]](
          checkForMismatches(concoctedKeyFields, valueFields),
          keyFieldsCheckedUnsupportedLogicalType,
          valueNullableFieldCheckedForDefault,
          valueFieldsCheckedUnsupportedLogicalType,
          kv._1,
          kv._2).flatten
      }
      validationErrors.flatMap { validationErrors =>
        if (validationErrors.nonEmpty) Bracket[F, Throwable].raiseError(ValidationErrors(validationErrors)) else Bracket[F, Throwable].pure(())
      }
    } else Bracket[F, Throwable].raiseError(KeyAndValueNotRecordType)
  }

  private def validateKeyAndValueSchemasForRecordRecordTypes(schemas: Schemas, subject: Subject, isKSQLTopic: Boolean): F[Unit] = {
    val keyFields = schemas.key.getFields.asScala.toList
    val valueFields = schemas.value.getFields.asScala.toList
    val keyFieldIsEmpty: Option[IncompatibleSchemaException] = if (keyFields.isEmpty) {
      IncompatibleSchemaException("Must include Fields in Key").some
    } else None
    val validationErrors = for {
      kv <- validateSchemaEvolutions(schemas, subject)
    } yield {
      val keyFieldsCheckedForNull = if(isKSQLTopic) none else checkForNullableKeyFields(keyFields)
      val valueNullableFieldCheckedForDefault = checkForDefaultNullableValueFields(valueFields)
      val keyFieldsCheckedUnsupportedLogicalType = checkForUnsupportedLogicalType(keyFields)
      val valueFieldsCheckedUnsupportedLogicalType = checkForUnsupportedLogicalType(valueFields)
      List[Option[RuntimeException]](
        checkForMismatches(keyFields, valueFields),
        keyFieldIsEmpty,
        keyFieldsCheckedForNull,
        keyFieldsCheckedUnsupportedLogicalType,
        valueNullableFieldCheckedForDefault,
        valueFieldsCheckedUnsupportedLogicalType,
        kv._1,
        kv._2).flatten
    }
    validationErrors.flatMap { validationErrors =>
      if (validationErrors.nonEmpty) Bracket[F, Throwable].raiseError(ValidationErrors(validationErrors)) else Bracket[F, Throwable].pure(())
    }
  }

  private def validateKeyAndValueSchemas(request: TopicMetadataV2Request, subject: Subject): Resource[F, Unit] = {
    val schemas = request.schemas
    val isKSQLTopic = request.tags.contains("KSQL")
    val completedValidations: F[Unit] = (schemas.key.getType, schemas.value.getType) match {
      case (Schema.Type.RECORD, Schema.Type.RECORD) =>
        validateKeyAndValueSchemasForRecordRecordTypes(schemas, subject, isKSQLTopic)
      case (Schema.Type.STRING, Schema.Type.RECORD) =>
        validateKeyAndValueSchemasForStringRecordTypes(schemas, subject, isKSQLTopic)
      case _ => Bracket[F, Throwable].raiseError(KeyAndValueNotRecordType)
    }
    Resource.liftF(completedValidations)
  }

  def createTopicFromMetadataOnly(
                                   topicName: Subject,
                                   createTopicRequest: TopicMetadataV2Request): F[Unit] = {
    (for {
      _ <- validateKeyAndValueSchemas(createTopicRequest, topicName)
      _ <- Resource.liftF(publishMetadata(topicName, createTopicRequest))
    } yield()).use(_ => Bracket[F, Throwable].unit)
  }

  def createTopic(
                   topicName: Subject,
                   createTopicRequest: TopicMetadataV2Request,
                   defaultTopicDetails: TopicDetails
                 ): F[Unit] = {

    if(createTopicRequest.streamType == StreamTypeV2.Entity) {
      defaultTopicDetails.copy(partialConfig = defaultTopicDetails.partialConfig + ("cleanup.policy" -> "compact"))
    }
    val td = createTopicRequest.numPartitions.map(numP =>
      defaultTopicDetails.copy(numPartitions = numP.value)).getOrElse(defaultTopicDetails)
    (for {
      _ <- validateKeyAndValueSchemas(createTopicRequest, topicName)
      _ <- registerSchemas(
        topicName,
        createTopicRequest.schemas.key,
        createTopicRequest.schemas.value
      )
      _ <- createTopicResource(topicName, td)
      _ <- Resource.liftF(publishMetadata(topicName, createTopicRequest))
    } yield ()).use(_ => Bracket[F, Throwable].unit)
  }
}

object CreateTopicProgram {
  final case class KeyAndValueMismatch(fieldName: String, keyFieldSchema: Schema, valueFieldSchema: Schema)
  final case class NullableField(fieldName: String, keyFieldSchema: Schema)
  final case class NullableFieldWithoutDefaultValue(fieldName: String, valueFieldSchema: Schema)
  final case class KeyHasNullableFields(errors: List[NullableField]) extends
    RuntimeException(
      (List("Fields within the key object cannot be nullable:", "Field Name\tKey Schema") ++ errors.map(e => s"${e.fieldName}\t${e.keyFieldSchema.toString}")).mkString("\n")
    )
  final case class NullableFieldsNeedDefaultValue(error: List[NullableFieldWithoutDefaultValue]) extends RuntimeException(
    (List("Nullable fields should have default value:", "Field Name\tValue Schema") ++ error.map(e => s"${e.fieldName}\t${e.valueFieldSchema.toString}")).mkString("\n")
  )
  final case class IncompatibleKeyAndValueFieldNames(errors: List[KeyAndValueMismatch]) extends
    RuntimeException(
      (List("Fields with same names in key and value schemas must have same type:", "Field Name\tKey Schema\tValue Schema") ++ errors.map(e => s"${e.fieldName}\t${e.keyFieldSchema.toString}\t${e.valueFieldSchema}")).mkString("\n")
    )
  final case class ValidationErrors(listOfRuntimeException: List[RuntimeException]) extends
    RuntimeException(listOfRuntimeException.map(_.getMessage).mkString("\n"))
  final case class UnsupportedLogicalType(unsupportedFields: List[Schema.Field]) extends
    RuntimeException(
      unsupportedFields.map(f => s"Field named '${f.name()}' has unsupported logical type '${getLogicalType(f)}'").mkString("\n")
    )
  val KeyAndValueNotRecordType =
    IncompatibleSchemaException("Your key and value schemas must each be of type record. If you are adding metadata for a topic you created externally, you will need to register new key and value schemas")

  protected def getLogicalType(field: Schema.Field): Option[String] =
    Option(field.schema().getLogicalType)
      .fold(Option(field.schema().getProp("logicalType")))(_.getName.some)
}
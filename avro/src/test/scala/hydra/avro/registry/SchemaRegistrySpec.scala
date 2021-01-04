package hydra.avro.registry

import cats.effect.{IO, Sync}
import cats.syntax.all._
import cats.{Applicative, Monad, MonadError}
import hydra.avro.registry.SchemaRegistry.IncompatibleSchemaException
import org.apache.avro.Schema.Parser
import org.apache.avro.{Schema, SchemaBuilder}
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

class SchemaRegistrySpec extends AnyFlatSpecLike with Matchers {

  private def getSchema[F[_]: Applicative](name: String): F[Schema] =
    Applicative[F].pure {
      SchemaBuilder
        .record(name)
        .fields()
        .name("isTrue")
        .`type`()
        .stringType()
        .noDefault()
        .endRecord()
    }

  private def testAddSubject[F[_]: Monad](
      schemaRegistry: SchemaRegistry[F]
  ): F[Unit] = {
    val subject = "testSubjectAdd-value"
    for {
      schema <- getSchema[F]("testSchemaAdd")
      _ <- schemaRegistry.registerSchema(subject, schema)
      allVersions <- schemaRegistry.getAllVersions(subject)
    } yield {
      it must "add a schema" in {
        allVersions shouldBe List(1)
      }
    }
  }

  private def testAddEvolution[F[_]: Monad](
                                           schemaRegistry: SchemaRegistry[F]
                                         ): F[Unit] = {
    def recordBuilder(name: String): SchemaBuilder.FieldAssembler[Schema] = {
      SchemaBuilder.record(name).fields().requiredString("id")
    }
    val subject = "testSubjectAdd-value"
    val name = "testSubjectAdd"

    val schema = recordBuilder(name).endRecord()
    val evolvedSchema = recordBuilder(name).nullableBoolean("nullBool", false).endRecord()

    for {
      _ <- schemaRegistry.registerSchema(subject, schema)
      _ <- schemaRegistry.registerSchema(subject, evolvedSchema)
      allVersions <- schemaRegistry.getAllVersions(subject)
    } yield {
      it must "add a schema with an evolved schema" in {
        allVersions shouldBe List(1, 2)
      }
    }
  }

  private def testNoAddKeyNoEvolution[F[_]: Monad](
                                             schemaRegistry: SchemaRegistry[F]
                                           ): F[Unit] = {
    def recordBuilder(name: String): SchemaBuilder.FieldAssembler[Schema] = {
      SchemaBuilder.record(name).fields().requiredString("id")
    }
    val subject = "testSubjectAdd-key"

    val schema = recordBuilder("schemaNameTest").endRecord()

    for {
      _ <- schemaRegistry.registerSchema(subject, schema)
      _ <- schemaRegistry.registerSchema(subject, schema)
      allVersions <- schemaRegistry.getAllVersions(subject)
    } yield {
      it must "not add a schema when no key evolution takes place" in {
        allVersions shouldBe List(1)
      }
    }
  }

  private def testErrorKeyEvolution[F[_]: MonadError[*[_], Throwable]](
                                                    schemaRegistry: SchemaRegistry[F]
                                                  ): F[Unit] = {
    def recordBuilder(name: String): SchemaBuilder.FieldAssembler[Schema] = {
      SchemaBuilder.record(name).fields().requiredString("id")
    }
    val subject = "testSubjectAdd-key"

    val schema = recordBuilder("schemaName").endRecord()
    val evolvedSchema = recordBuilder("schemaName").nullableBoolean("nullBool", false).endRecord()

    for {
      _ <- schemaRegistry.registerSchema(subject, schema)
      error <- schemaRegistry.registerSchema(subject, evolvedSchema).attempt
      allVersions <- schemaRegistry.getAllVersions(subject)
    } yield {
      it must "return an error when a key schema evolution is attempted" in {
        error shouldBe IncompatibleSchemaException("Key schema evolutions are not permitted unless to add inconsequential elements i.e. doc fields.").asLeft
        allVersions shouldBe List(1)
      }
    }
  }

  def recordBuilder(name: String, withDoc: Boolean = false, doc: String = ""): Schema = {
    val testSchemaString =
      s"""{
            "type":"record",
            "name":"$name",
            "fields":[
              {
                "name":"id",
                "type":"string"
              }
            ]
          }"""

    val testSchemaStringWithDoc =
      s"""{
            "type":"record",
            "name":"$name",
            "fields":[
              {
                "name":"id",
                "type":"string",
                "doc":"$doc"
              }
            ]
          }"""

    new Parser().parse(if (withDoc) testSchemaStringWithDoc else testSchemaString)
  }

  private def testInconsequentialKeyEvolutions[F[_]: MonadError[*[_], Throwable]: Sync]: F[Unit] = {
    val firstIteration = recordBuilder("schemaName")
    val evolvedSchema = recordBuilder("schemaName", withDoc = true, "Documentation")
    val lastIteration = recordBuilder("schemaName", withDoc = true, "Documentation Updated")

    val schemasF: F[List[Schema]] = Applicative[F].pure(List[Schema](firstIteration, evolvedSchema))
    val subject = "testSubjectAdd-key"

    SchemaRegistry.CheckKeySchemaEvolution(schemasF).checkKeyEvolution(subject, lastIteration).map { _ =>
      it must "succeed when inconsequential updates are made to the key schema" in {
        succeed
      }
    }
  }

  private def testNoPriorKeySchema[F[_]: MonadError[*[_], Throwable]: Sync]: F[Unit] = {
    val lastIteration = recordBuilder("schemaName", withDoc = true, "Documentation Updated")

    val schemasF: F[List[Schema]] = Applicative[F].pure(List[Schema]())
    val subject = "testSubjectAdd-key"

    SchemaRegistry.CheckKeySchemaEvolution(schemasF).checkKeyEvolution(subject, lastIteration).map { _ =>
      it must "succeed when no previous key schemas exist" in {
        succeed
      }
    }
  }

  private def testValueSchemaBeingChecked[F[_]: MonadError[*[_], Throwable]: Sync]: F[Unit] = {
    val lastIteration = recordBuilder("schemaName", withDoc = true, "Documentation Updated")
    val schemasF: F[List[Schema]] = Applicative[F].pure(List[Schema]())
    val subject = "testSubjectAdd-key"

    SchemaRegistry.CheckKeySchemaEvolution(schemasF).checkKeyEvolution(subject, lastIteration).map { _ =>
      it must "succeed when value schema is being checked" in {
        succeed
      }
    }
  }


  private def testDeleteSchemaVersion[F[_]: Monad](
      schemaRegistry: SchemaRegistry[F]
  ): F[Unit] = {
    val subject = "testSubjectDelete-value"
    for {
      schema <- getSchema[F]("testSubjectDelete")
      _ <- schemaRegistry.registerSchema(subject, schema)
      version <- schemaRegistry.getVersion(subject, schema)
      _ <- schemaRegistry.deleteSchemaOfVersion(subject, version)
      allVersions <- schemaRegistry.getAllVersions(subject)
    } yield {
      it must "delete a schema version" in {
        allVersions shouldBe empty
      }
    }
  }

  private def testGetAllSubjects[F[_]: Monad](
      schemaRegistry: SchemaRegistry[F]
  ): F[Unit] = {
    val subject = "testGetAllSubjects-value"
    for {
      schema <- getSchema[F]("testGetAllSubjects")
      allSubjectsEmpty <- schemaRegistry.getAllSubjects
      _ <- schemaRegistry.registerSchema(subject, schema)
      allSubjectsOne <- schemaRegistry.getAllSubjects
      version <- schemaRegistry.getVersion(subject, schema)
      _ <- schemaRegistry.deleteSchemaOfVersion(subject, version)
      allSubjectsAfterDelete <- schemaRegistry.getAllSubjects
    } yield {
      it must "get all subjects when no subjects exist" in {
        allSubjectsEmpty shouldBe empty
      }
      it must "get all subjects when a subject exists" in {
        allSubjectsOne shouldBe List(subject)
      }
      it must "get all subjects even after their schemas have been deleted" in {
        allSubjectsAfterDelete shouldBe List(subject)
      }
    }
  }

  private def testSchemaEvolutionValidation[F[_]: Monad]: F[Unit] = {
    def recordBuilder(name: String): SchemaBuilder.FieldAssembler[Schema] = {
      SchemaBuilder.record(name).fields().requiredString("id")
    }
    val baseRecord1 = recordBuilder("BaseRecord1").endRecord()
    val addDefaultedField = recordBuilder("BaseRecord1").nullableInt("newInt", 0).endRecord()
    val shouldBeSuccess = SchemaRegistry.validate(addDefaultedField, baseRecord1 :: Nil)

    val baseRecord2 = recordBuilder("BaseRecord2").endRecord()
    val incompatibleRequiredAddEvolution = recordBuilder("BaseRecord2").requiredBoolean("reqBool").endRecord()
    val incompatibleNullableNoDefaultEvolution = recordBuilder("BaseRecord2")
      .name("nullBool").`type`().booleanType().noDefault().endRecord()
    val requiredFieldFailure = SchemaRegistry.validate(incompatibleRequiredAddEvolution, baseRecord2 :: Nil)
    val noDefaultFailure = SchemaRegistry.validate(incompatibleNullableNoDefaultEvolution, baseRecord2 :: Nil)

    val baseRecord3 = recordBuilder("BaseRecord3").nullableInt("nullableInt", 10).endRecord()
    val validFieldRemoval = recordBuilder("BaseRecord3").endRecord()
    val defaultRemovalValid = SchemaRegistry.validate(validFieldRemoval, baseRecord3 :: Nil)


    Applicative[F].pure {
      it must "validate the schema evolutions" in {
        assert(shouldBeSuccess)
        assert(!requiredFieldFailure)
        assert(!noDefaultFailure)
        assert(defaultRemovalValid)
      }
    }
  }

  private def testAddFailingEvolution[F[_]: Monad](
                                             schemaRegistryIO: F[SchemaRegistry[F]]
                                           ): F[Unit] = {
    def recordBuilder(name: String): SchemaBuilder.FieldAssembler[Schema] = {
      SchemaBuilder.record(name).fields().requiredString("id")
    }
    val subject = "testSubjectAdd"

    val schema = recordBuilder("testName").endRecord()
    val invalidSchemaEvolution = recordBuilder("testName").requiredBoolean("nullBool").endRecord()

    for {
      schemaRegistry <- schemaRegistryIO
      _ <- schemaRegistry.registerSchema(subject, schema)
      _ <- schemaRegistry.registerSchema(subject, invalidSchemaEvolution)
    } yield ()
  }

  private def runTests[F[_]: MonadError[*[_], Throwable]: Sync](
      schemaRegistry: F[SchemaRegistry[F]]
  ): F[Unit] = {
    for {
      _ <- schemaRegistry.flatMap(testAddSubject[F])
      _ <- schemaRegistry.flatMap(testAddEvolution[F])
      _ <- schemaRegistry.flatMap(testNoAddKeyNoEvolution[F])
      _ <- schemaRegistry.flatMap(testErrorKeyEvolution[F])
      _ <- schemaRegistry.flatMap(testDeleteSchemaVersion[F])
      _ <- schemaRegistry.flatMap(testGetAllSubjects[F])
      _ <- testInconsequentialKeyEvolutions[F]
      _ <- testNoPriorKeySchema[F]
      _ <- testValueSchemaBeingChecked[F]
      _ <- testSchemaEvolutionValidation[F]
    } yield ()
  }

  runTests(SchemaRegistry.test[IO]).unsafeRunSync()

  it must "catch incompatibleSchemaException" in {
    a[IncompatibleSchemaException] should be thrownBy testAddFailingEvolution(SchemaRegistry.test[IO]).unsafeRunSync()
  }
}

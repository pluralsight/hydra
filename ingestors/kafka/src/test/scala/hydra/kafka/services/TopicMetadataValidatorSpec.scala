package hydra.kafka.services

import hydra.core.marshallers.GenericSchema
import hydra.kafka.services.ErrorMessages._
import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpecLike

import scala.util.{Failure, Success}

class TopicMetadataValidatorSpec extends AnyFlatSpecLike with Matchers {

  "A TopicNameValidator" should "return Valid for a valid topic" in {
    TopicMetadataValidator.validate(
      Some(GenericSchema("TestEntity", "dvs.test_topic.v1"))
    ) shouldBe Success(Valid)
  }

  it should "return Invalid for a topic name longer than 249 characters" in {
    TopicMetadataValidator.validate(
      Some(GenericSchema("TestEntity", "exp.test." + "a" * 250))
    ) shouldBe Failure(SchemaValidatorException(Seq(LengthError)))
  }

  it should "return Invalid for a topic containing invalid characters" in {
    TopicMetadataValidator.validate(
      Some(GenericSchema("TestEntity", "exp.test.Test(Topic)"))
    ) shouldBe Failure(SchemaValidatorException(Seq(InvalidCharacterError)))
  }

  it should "return invalid if doesn't start with a valid org prefix" in {
    TopicMetadataValidator.validate(
      Some(GenericSchema("TestEntity", "false.test.TestTopic"))
    ) shouldBe Failure(SchemaValidatorException(Seq(BadOrgError)))
  }

  it should "be properly formatted by containing at least 3 segments" in {
    TopicMetadataValidator.validate(Some(GenericSchema("TestEntity", "exp"))) shouldBe Failure(
      SchemaValidatorException(Seq(BadTopicFormatError))
    )
  }

  it should "return multiple errors if validation fails for multiple reasons" in {
    TopicMetadataValidator.validate(
      Some(GenericSchema("TestEntity", "falsetestTestTopic"))
    ) shouldBe Failure(
      SchemaValidatorException(Seq(BadOrgError, BadTopicFormatError))
    )
  }

  it should "return no errors if lgl is used as the organization" in {
    TopicMetadataValidator.validate(
      Some(GenericSchema("TestEntity", "lgl.test.test_topic.v1"))
    ) shouldBe Success(Valid)
  }

  it should "return Invalid for namespace or name containing hyphens" in {
    TopicMetadataValidator.validate(
      Some(GenericSchema("Test-Entity", "exp.test.l-1325"))
    ) should not be Failure(
      SchemaValidatorException(Seq(InvalidCharacterError))
    )
  }

  it should "return Invalid for missing generic schema" in {
    TopicMetadataValidator.validate(None) shouldBe Failure(
      SchemaValidatorException(Seq(SchemaError))
    )
  }

}

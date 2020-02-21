package hydra.kafka.services

import hydra.core.marshallers.GenericSchema

import scala.util.{Failure, Try}

object TopicMetadataValidator {

  import ErrorMessages._

  private val subjectValidationFunctions: Seq[String => ValidationResponse] =
    List(
      topicIsTooLong,
      topicContainsInvalidChars,
      validOrg,
      validFormat
    )

  def validate(gOpt: Option[GenericSchema]): Try[ValidationResponse] = {
    gOpt match {
      case Some(g) =>
        subjectValidationFunctions
          .map(f => f(s"${g.namespace}.${g.name}"))
          .collect {
            case r: Invalid => r
          } match {
          case respSeq: Seq[ValidationResponse] if respSeq.nonEmpty =>
            Failure(
              SchemaValidatorException(respSeq.map(invalid => invalid.reason))
            )
          case _ => scala.util.Success(Valid)
        }
      case None =>
        Failure(SchemaValidatorException(SchemaError :: Nil))
    }
  }

  private def topicIsTooLong(topic: String): ValidationResponse = {
    topic match {
      case s: String if s.length <= 249 => Valid
      case _                            => Invalid(LengthError)
    }
  }

  private def topicContainsInvalidChars(topic: String): ValidationResponse = {
    if (topic.matches("""^[a-zA-Z0-9._\-]*$""")) {
      Valid
    } else {
      Invalid(InvalidCharacterError)
    }
  }

  private def validOrg(topic: String): ValidationResponse = {
    val validOrgs = Set("exp", "rev", "fin", "mkg", "pnp", "sbo", "dvs", "lgl")

    val orgOpt = topic.split("\\.").headOption

    orgOpt match {
      case Some(org) =>
        if (validOrgs contains org) {
          Valid
        } else {
          Invalid(BadOrgError)
        }
      case None => Invalid(BadOrgError)
    }
  }

  private def validFormat(topic: String): ValidationResponse = {
    topic.split("\\.").filterNot(_ == "").toList match {
      case Nil                             => Invalid(BadTopicFormatError)
      case l: List[String] if l.length < 3 => Invalid(BadTopicFormatError)
      case _                               => Valid
    }
  }
}

object ErrorMessages {
  val SchemaError: String = "Schema does not contain a namespace and a name"

  val LengthError: String =
    "Schema namespace +  schema name longer than 249 characters"

  val InvalidCharacterError: String =
    "Schema namespace + schema name may only contain letters, numbers and the characters '.'," +
      " '_'" + " '-'"

  val BadOrgError: String =
    "Schema namespace must begin with one of the following organizations: exp | rev | fin |" +
      " mkg | pnp | sbo | dvs"

  val BadTopicFormatError: String =
    "Schema must be formatted as <organization>.<product|context|" +
      "team>[.<version>].<entity> where <entity> is the name and the rest is the namespace of the schema"
}

sealed trait ValidationResponse

case object Valid extends ValidationResponse

case class Invalid(reason: String) extends ValidationResponse

case class SchemaValidatorException(reasons: Seq[String])
    extends RuntimeException

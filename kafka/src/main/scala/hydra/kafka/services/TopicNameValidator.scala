package hydra.kafka.services

import scala.util.{Failure, Try}


object TopicNameValidator {

  import ErrorMessages._

  private val validationFunctions = List(
    topicIsTooLong _,
    topicContainsInvalidChars _,
    validOrg _,
    validFormat _
  )

  def validate(topic: String): Try[ValidationResponse] = {
    validationFunctions
      .map(f => f(topic))
      .collect {
        case r: Invalid => r
      } match {
      case respSeq: Seq[ValidationResponse] if respSeq.nonEmpty =>
        Failure(TopicNameValidatorException(respSeq.map(invalid => invalid.reason)))
      case _ => scala.util.Success(Valid)
    }
  }

  private def topicIsTooLong(topic: String): ValidationResponse = {
    topic match {
      case s: String if s.length <= 249 => Valid
      case _ => Invalid(LengthError)
    }
  }

  private def topicContainsInvalidChars(topic: String): ValidationResponse = {
    if (topic.matches("^[a-zA-Z0-9\\.\\-\\_]*$")) {
      Valid
    }
    else {
      Invalid(InvalidCharacterError)
    }
  }

  private def validOrg(topic: String): ValidationResponse = {
    val validOrgs = Set("exp", "rev", "fin", "mkg", "pnp", "sbo", "dvs")

    val orgOpt = topic.split("\\.").headOption

    orgOpt match {
      case Some(org) =>
        if (validOrgs contains org) {
          Valid
        }
        else {
          Invalid(BadOrgError)
        }
      case None => Invalid(BadOrgError)
    }
  }

  private def validFormat(topic: String): ValidationResponse = {
    topic.split("\\.").toList match {
      case Nil => Invalid(BadTopicFormatError)
      case l: List[String] if l.length < 3 => Invalid(BadTopicFormatError)
      case _ => Valid
    }
  }
}

object ErrorMessages {
  val LengthError: String = "Topic name longer than 249 characters"

  val InvalidCharacterError: String = "Topic may only contain letters, numbers and the characters '.'," +
    " '_', '-'"

  val BadOrgError: String = "Topic must begin with one of the following organizations: exp | rev | fin |" +
    " mkg | pnp | sbo | dvs"

  val BadTopicFormatError: String = "Topic must be formatted as <organization>.<product|context|" +
    "team>[.<version>].<entity>"
}

sealed trait ValidationResponse

case object Valid extends ValidationResponse

case class Invalid(reason: String) extends ValidationResponse

case class TopicNameValidatorException(reasons: Seq[String]) extends RuntimeException
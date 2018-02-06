package hydra.ingest.http

import scala.concurrent.{ ExecutionContext, Future }

import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import hydra.common.logging.LoggingAdapter
import hydra.core.akka.SchemaRegistryActor.{ RegisterSchema, RegisteredSchema }
import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import org.apache.avro.SchemaParseException

//TODO(BAP):  once all routes are using the facade, bring the schemaNameSuffix into here
class SchemasEndpointFacade(schemaRegistryActor: ActorRef) extends LoggingAdapter {

  val validSchemaNameRegex = "^[a-zA-Z0-9]*$".r
  val schemaParser = new Schema.Parser()
  val schemaNameSuffix = "-value"

  def isValidSchemaName(schemaName: String) = {
    validSchemaNameRegex.pattern.matcher(schemaName).matches
  }

  def registerSchema(schemaJson: String)(implicit ec: ExecutionContext, timeout: Timeout): Future[RegisteredSchema] = {
    val schema = schemaParser.parse(schemaJson)
    if (!isValidSchemaName(schema.getName()))
      throw new SchemaParseException("Schema name may only contain letters and numbers.")

    val name = schema.getNamespace() + "." + schema.getName()
    val subject = name + schemaNameSuffix

    log.debug(s"Registering schema $name: $schemaJson")
    (schemaRegistryActor ? RegisterSchema(subject, schema)).mapTo[RegisteredSchema]
  }
}

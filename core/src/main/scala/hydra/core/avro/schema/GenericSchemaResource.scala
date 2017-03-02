package hydra.core.avro.schema

import java.io.InputStream

import org.apache.avro.Schema.Parser
import org.springframework.core.io.{DefaultResourceLoader, Resource}

/**
  * Created by alexsilva on 2/21/17.
  */
case class GenericSchemaResource(source: Resource, id: Int = 0, version: Int = 0) extends SchemaResource {

  val location = source.getFilename

  lazy val schema = new Parser().parse(getInputStream)

  override def getDescription: String = source.getDescription

  override def getInputStream: InputStream = source.getInputStream
}

object GenericSchemaResource {
  def apply(location: String, id: Int, version: Int): GenericSchemaResource =
    GenericSchemaResource(new DefaultResourceLoader().getResource(location), id, version)
}


package hydra.avro.registry

import java.io.{ByteArrayInputStream, InputStream}
import java.net.URL

import hydra.avro.resource.SchemaResource
import org.apache.avro.Schema

/**
  * Created by alexsilva on 2/21/17.
  */
case class RegistrySchemaResource(registryUrl: String, subject: String, id: Int, version: Int, schema: Schema)
  extends SchemaResource {

    val location = s"$registryUrl/schemas/ids/$id"

    override val getDescription: String = s"Schema with subject $subject, version $version loaded " +
      s"from the schema registry at $registryUrl."

    override def getInputStream: InputStream = new ByteArrayInputStream(schema.toString.getBytes())

    override def getURL: URL = new URL(location)

    override def exists(): Boolean = true
  }

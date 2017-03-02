package hydra.core.avro.registry

import java.io.{ByteArrayInputStream, InputStream}
import java.net.URL

import hydra.core.avro.schema.SchemaResource
import org.apache.avro.Schema

/**
  * Created by alexsilva on 2/21/17.
  */
case class RegistrySchemaResource(registry: String, subject: String, id: Int, version: Int, schema: Schema)
  extends SchemaResource {

    val location = s"$registry/ids/$id"

    override val getDescription: String = s"Schema with subject $subject, version $version loaded " +
      s"from registry at $registry."

    override def getInputStream: InputStream = new ByteArrayInputStream(schema.toString.getBytes())

    override def getURL: URL = new URL(location)

    override def exists(): Boolean = true
  }

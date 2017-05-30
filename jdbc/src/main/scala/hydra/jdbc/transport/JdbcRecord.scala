package hydra.jdbc.transport

import hydra.core.transport.{HydraRecord, RetryStrategy}
import org.apache.avro.generic.GenericRecord

/**
  * Defines a JDBC record.
  * Created by alexsilva on 5/19/17.
  *
  * @param destination   The table name
  * @param payload       The avro generic record associated with this record.
  * @param retryStrategy The Hydra retry strategy
  * @param key           The name of the primary key column (if any)
  */
case class JdbcRecord(destination: String,
                      payload: GenericRecord,
                      retryStrategy: RetryStrategy,
                      key: Option[String]) extends HydraRecord[String, GenericRecord]


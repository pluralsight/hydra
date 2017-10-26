package hydra.avro.io

import hydra.avro.io.SaveMode.SaveMode
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

/**
  * Created by alexsilva on 7/16/17.
  */
trait RecordWriter {

  /**
    * Writes a record to the underlying store.
    *
    * The Unit return type means the actual semantics of this method may vary;
    * for instance, on implementation using record batches, any errors/exceptions will not be reported
    * until the batch is executed.
    *
    * @param record
    */
  def add(record: GenericRecord): Unit

  /**
    * Flushes any cache/record batch to the underlying store.
    *
    * This is an optional operation.
    */
  def flush(): Unit

  /**
    * Closes this writer, also triggering a flush() if needed.
    */
  def close(): Unit

  /**
    * The underlying schema this record writer is expecting to receive.
    *
    * This control the creation of any underlying data stores, such as tables in a database.
    */
  def schema: Schema

  /**
    * @return The save mode for this writer. Used when the writer is being initialized.
    */
  def mode: SaveMode

}

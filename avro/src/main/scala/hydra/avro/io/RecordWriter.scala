package hydra.avro.io

import hydra.avro.io.RecordWriter.Operation
import hydra.avro.io.SaveMode.SaveMode
import hydra.avro.util.SchemaWrapper
import org.apache.avro.Schema

/**
  * Created by alexsilva on 7/16/17.
  */
trait RecordWriter {

  /**
    * Adds an operation to the current batch.
    *
    * The Unit return type means the actual semantics of this method may vary;
    * for instance, on implementation using batches, any errors/exceptions will not be reported
    * until the batch is executed.
    *
    * @param operation
    */
  def addBatch(operation: Operation): Unit

  /**
    * Immediately writes a single record to the underlying record store.
    *
    * @param operation
    */
  def execute(operation: Operation)

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
  def schema: SchemaWrapper

  /**
    * @return The save mode for this writer. Used when the writer is being initialized.
    */
  def mode: SaveMode
}

object RecordWriter {

  trait Operation {
    def schema: Schema
  }

}



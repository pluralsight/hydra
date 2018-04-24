package hydra.sql

import java.sql.BatchUpdateException

import hydra.avro.convert.IsoDate
import hydra.avro.io.SaveMode.SaveMode
import hydra.avro.io._
import hydra.avro.util.{AvroUtils, SchemaWrapper}
import hydra.common.util.TryWith
import org.apache.avro.generic.GenericRecord
import org.apache.avro.{LogicalTypes, Schema}
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.util.Failure
import scala.util.control.NonFatal


/**
  * Created by alexsilva on 7/11/17.
  *
  * A batch size of 0 means that this class will never do any executeBatch and that external clients need to call
  * flush()
  *
  * If the primary keys are provided as a constructor argument, it overrides anything that
  * may have been provided by the schema.
  *
  * @param settings        The JdbcWriterSettings to be used
  * @param schema          The initial schema to use when creating/updating/inserting records.
  * @param mode            See [hydra.avro.io.SaveMode]
  * @param tableIdentifier The table identifier; defaults to using the schema's name if none provided.
  */
class JdbcRecordWriter(val settings: JdbcWriterSettings,
                       val connectionProvider: ConnectionProvider,
                       val schema: SchemaWrapper,
                       val mode: SaveMode = SaveMode.ErrorIfExists,
                       tableIdentifier: Option[TableIdentifier] = None) extends RecordWriter with JdbcHelper {

  import JdbcRecordWriter._

  logger.debug("Initializing JdbcRecordWriter")

  private val batchSize = settings.batchSize

  private val syntax = settings.dbSyntax

  private val dialect = JdbcDialects.get(connectionProvider.connectionUrl)

  private val store: Catalog = new JdbcCatalog(connectionProvider, syntax, dialect)

  private val tableId = tableIdentifier.getOrElse(TableIdentifier(schema.getName))

  private val records = new mutable.ArrayBuffer[GenericRecord]()

  private var currentSchema = schema

  private val tableObj: Table = {
    val tableExists = store.tableExists(tableId)
    mode match {
      case SaveMode.ErrorIfExists if tableExists =>
        throw new AnalysisException(s"Table ${tableId.table} already exists.")
      case SaveMode.Overwrite => //todo: truncate table
        Table(tableId.table, schema, tableId.database)
      case _ =>
        val table = Table(tableId.table, schema, tableId.database)
        store.createOrAlterTable(table)
        table
    }
  }

  private val name = syntax.format(tableObj.name)

  private var valueSetter = new AvroValueSetter(schema, dialect)

  private var upsertStmt = dialect.upsert(syntax.format(name), schema, syntax)

  //since changing pks on a table isn't supported, this can be a val
  private val deleteByPkStmt =
    schema.primaryKeys.headOption.map(_ => dialect.deleteStatement(syntax.format(name), schema.primaryKeys, syntax))

  private def connection = connectionProvider.getConnection

  override def batch(operation: Operation): Unit = {
    operation match {
      case Upsert(record) => add(record)
      case DeleteByKey(fields) => //TODO: implement delete
    }
  }

  private def add(record: GenericRecord): Unit = {
    if (AvroUtils.areEqual(currentSchema.schema, record.getSchema)) {
      records += record
      if (batchSize > 0 && records.size >= batchSize) flush()
    }
    else {
      // Each batch needs to have the same dbInfo, so get the buffered records out, reset state if possible,
      // add columns and re-attempt the add
      flush()
      updateDb(record)
      add(record)
    }
  }

  private def updateDb(record: GenericRecord): Unit = synchronized {
    val cpks = currentSchema.primaryKeys
    val wrapper = SchemaWrapper.from(record.getSchema, cpks)
    store.createOrAlterTable(Table(tableId.table, wrapper))
    currentSchema = wrapper
    upsertStmt = dialect.upsert(syntax.format(name), currentSchema, syntax)
    valueSetter = new AvroValueSetter(currentSchema, dialect)
  }

  /**
    * Convenience method to write exactly one record to the underlying database.
    *
    * @param record
    */
  private def upsert(record: GenericRecord): Unit = {
    if (AvroUtils.areEqual(currentSchema.schema, record.getSchema)) {
      TryWith(connection.prepareStatement(upsertStmt)) { pstmt =>
        valueSetter.bind(record, pstmt)
        pstmt.executeUpdate()
      }.get //TODO: better error handling here, we do the get just so that we throw an exception if there is one.
    }
    else {
      updateDb(record)
      upsert(record)
    }
  }

  override def execute(operation: Operation): Unit = {
    operation match {
      case Upsert(record) => upsert(record)
      case DeleteByKey(fields) => throw new UnsupportedOperationException("Not supported")
    }
  }

  def flush(): Unit = synchronized {
    val conn = connectionProvider.getConnection
    val supportsTransactions = try {
      conn.getMetaData().supportsDataManipulationTransactionsOnly() ||
        conn.getMetaData().supportsDataDefinitionAndDataManipulationTransactions()

    } catch {
      case NonFatal(e) =>
        JdbcRecordWriter.logger.warn("Exception while detecting transaction support", e)
        true
    }

    var committed = false

    if (supportsTransactions) {
      conn.setAutoCommit(false) // Everything in the same db transaction.
    }
    val pstmt = conn.prepareStatement(upsertStmt)
    records.foreach(valueSetter.bind(_, pstmt))
    try {
      pstmt.executeBatch()
      if (supportsTransactions) {
        conn.commit()
      }
      committed = true
    }
    catch {
      case e: BatchUpdateException =>
        logger.error("Batch update error", e.getNextException())
        conn.rollback()
        val recordsInError = handleBatchError(records)
        logger.error(s"The following records could not be replicated to table $name:")
        recordsInError.foreach(r => logger.error(s"${r._1.toString} - [${r._2.getMessage}]"))
      case e: Exception =>
        throw e
    }
    finally {
      if (!committed && supportsTransactions) {
        conn.rollback()
      }

      conn.setAutoCommit(true) //back
    }
    records.clear()
  }

  def close(): Unit = {
    flush()
  }

  /**
    * Try running the batch statements, one record at a time
    *
    * Returns the generic record(s) that caused the failure.
    */
  private[sql] def handleBatchError(records: Seq[GenericRecord]): Seq[(GenericRecord, Throwable)] = {
    records.map { record =>
      record -> TryWith(connectionProvider.getNewConnection()) { conn =>
        TryWith(conn.prepareStatement(upsertStmt)) { pstmt =>
          valueSetter.bind(record, pstmt)
          logger.debug(s"Trying to insert potentially invalid record $record")
          pstmt.executeUpdate()
        }
      }
    }.filter(_._2.isFailure).map(x => x._1 -> Failure(x._2.failed.get).exception)
  }
}

object JdbcRecordWriter {

  LogicalTypes.register(IsoDate.IsoDateLogicalTypeName, (_: Schema) => IsoDate)

  val logger = LoggerFactory.getLogger(getClass)
}
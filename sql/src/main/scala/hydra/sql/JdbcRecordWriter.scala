package hydra.sql

import java.sql.{BatchUpdateException, Connection, PreparedStatement}

import com.zaxxer.hikari.HikariDataSource
import hydra.avro.io.RecordWriter.Operation
import hydra.avro.io.SaveMode.SaveMode
import hydra.avro.io._
import hydra.avro.util.{AvroUtils, SchemaWrapper}
import hydra.common.util.TryWith
import org.apache.avro.Schema
import org.slf4j.LoggerFactory

import scala.collection.mutable


/**
  * Created by alexsilva on 7/11/17.
  *
  * If the primary keys are provided as a constructor argument, it overrides anything that
  * may have been provided by the schema.
  *
  * @param dataSource      The datasource to be used
  * @param schema          The initial schema to use when creating/updating/inserting records.
  * @param mode            See [hydra.avro.io.SaveMode]
  * @param dialect         The jdbc dialect to use.
  * @param dbSyntax        THe database syntax to use.
  * @param batchSize       The commit batch size; -1 to disable auto batching.
  * @param tableIdentifier The table identifier; defaults to using the schema's name if none provided.
  */
class JdbcRecordWriter(val dataSource: HikariDataSource,
                       val schema: SchemaWrapper,
                       val mode: SaveMode = SaveMode.ErrorIfExists,
                       dialect: JdbcDialect,
                       dbSyntax: DbSyntax = UnderscoreSyntax,
                       batchSize: Int = 50,
                       tableIdentifier: Option[TableIdentifier] = None)
  extends RecordWriter
    with JdbcHelper {

  require(batchSize >= 1, "Batch size must be greater than or equal to 1.")

  private val store: Catalog = new JdbcCatalog(dataSource, dbSyntax, dialect)

  private val tableId = tableIdentifier.getOrElse(TableIdentifier(schema.getName))

  private val pendingOperations = new mutable.ArrayBuffer[PendingOperation]()

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

  private val name = dbSyntax.format(tableObj.name)

  private var valueSetter = new AvroValueSetter(schema, dialect)

  private val statementCache = new mutable.HashMap[Class[_ <: Operation], PreparedStatement]()

  override def addBatch(op: Operation): Unit = {
    if (AvroUtils.areEqual(currentSchema.schema, op.schema)) {
      pendingOperations += PendingOperation(op)
      if (pendingOperations.size >= batchSize) flush()
    }
    else {
      // Each batch needs to have the same dbInfo, so get the buffered records out,
      // reset state if possible, add columns, and re-attempt the add
      refresh(op.schema)
      addBatch(op)
    }
  }

  override def execute(op: Operation): Unit = {
    if (!AvroUtils.areEqual(currentSchema.schema, op.schema)) refresh(op.schema)
    doExecute(Seq(PendingOperation(op)), true)
  }

  private def doExecute(ops: Seq[PendingOperation], autoCommit: Boolean): Unit = {
    TryWith(dataSource.getConnection) { conn =>
      var committed = false

      conn.setAutoCommit(autoCommit)

      ops.foreach { op =>
        val operation = op.operation
        val stmt = statementCache.getOrElse(operation.getClass, prepareStatement(conn, operation))
        valueSetter.bind(operation, stmt)
        op.batched = true
      }

      try {
        statementCache.values.foreach(_.executeBatch())

        if (!autoCommit) conn.commit()

        committed = true

        pendingOperations --= pendingOperations.filter(_.batched)
      }
      catch {
        case e: BatchUpdateException =>
          JdbcRecordWriter.logger.error("Batch update error", e.getNextException()); throw e
        case e: Exception =>
          throw e
      }
      finally {
        if (!committed) conn.rollback()
      }
    }
  }

  def flush(): Unit = synchronized {
    doExecute(pendingOperations, false)
  }

  private def refresh(schema: Schema) = synchronized {
    flush()
    updateDb(schema)
    statementCache.values.foreach(_.close())
    statementCache.clear()
  }

  private def updateDb(schema: Schema): Unit = {
    require(schema.getName == currentSchema.getName,
      "Updating a row with a different schema name is not " +
        s"supported. [${currentSchema.getName} -> ${schema.getName}]")
    val cpks = currentSchema.primaryKeys
    val wrapper = SchemaWrapper.from(schema, cpks)
    store.createOrAlterTable(Table(tableId.table, wrapper))
    currentSchema = wrapper
    valueSetter = new AvroValueSetter(currentSchema, dialect)
  }

  private[sql] def prepareStatement(conn: Connection, op: Operation) = {
    op match {
      case Upsert(_) => conn.prepareStatement(dialect.
        upsert(dbSyntax.format(name), currentSchema, dbSyntax))

      case Delete(schema, _) => conn.prepareStatement(dialect.
        deleteStatement(dbSyntax.format(name), currentSchema.primaryKeys, dbSyntax))

      case _ =>
        val opName = op.getClass.getCanonicalName
        throw new UnsupportedOperationException(s"Operation $opName is not supported.")
    }
  }


  def close(): Unit = {
    flush()
  }
}

case class PendingOperation(operation: Operation) {
  var batched: Boolean = false
}

object JdbcRecordWriter {

  val logger = LoggerFactory.getLogger(getClass)
}
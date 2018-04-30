package hydra.sql

import hydra.avro.io.SaveMode
import hydra.avro.io.SaveMode.SaveMode
import hydra.avro.util.SchemaWrapper
import hydra.sql.TableCreator._

class TableCreator(provider: ConnectionProvider, dbSyntax: DbSyntax,
                   dialect: JdbcDialect, parameters: Map[String, String]) {

  private val isTruncate = parameters.getOrElse(JdbcTruncate, "false").toBoolean &&
    (dialect isCascadingTruncateTable()).contains(false)

  def createOrAlterTable(mode: SaveMode, wrapper: SchemaWrapper): Unit = {
    val tableName = dbSyntax.format(wrapper.schema.getName)
    val conn = provider.getConnection
    val tableExists = JdbcUtils.tableExists(conn, dialect, tableName)
    val store = new JdbcCatalog(provider, UnderscoreSyntax, dialect)
    val tableId = TableIdentifier(tableName)
    val table = Table(tableId.table, wrapper, tableId.database)
    if (tableExists) {
      mode match {
        case SaveMode.Overwrite =>
          if (isTruncate) {
            // In this case, we should truncate table and then load.
            JdbcUtils.truncateTable(conn, dialect, tableName)
            store.createOrAlterTable(table)
          } else {
            // Otherwise, do not truncate the table, instead drop and recreate it
            JdbcUtils.dropTable(conn, tableName)
            store.createOrAlterTable(table)
          }

        case SaveMode.Append =>
          store.createOrAlterTable(table)

        case SaveMode.ErrorIfExists =>
          throw new AnalysisException(
            s"Table or view '$tableName' already exists. SaveMode: ErrorIfExists.")

        case SaveMode.Ignore =>
        // With `SaveMode.Ignore` mode, if table already exists, the save operation is expected
        // to not save the contents of the DataFrame and to not change the existing data.
        // Therefore, it is okay to do nothing here and then just return the relation below.
      }
    } else {
      store.createOrAlterTable(table)
    }
  }
}

object TableCreator {
  val JdbcTruncate = "truncate"
}

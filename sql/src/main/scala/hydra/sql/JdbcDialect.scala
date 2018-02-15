package hydra.sql

import org.apache.avro.Schema
import org.apache.avro.Schema.Field

/**
  * Created by alexsilva on 5/4/17.
  */
abstract class JdbcDialect extends Serializable {
  /**
    * The table name to be used when querying the resultset metadata API.
    *
    * @param tableName
    * @return
    */
  def tableNameForMetadataQuery(tableName: String) = tableName.toUpperCase

  /**
    * Check if this dialect instance can handle a certain jdbc url.
    *
    * @param url the jdbc url.
    * @return True if the dialect can be applied on the given jdbc url.
    * @throws NullPointerException if the url is null.
    */
  def canHandle(url: String): Boolean

  def caseSensitiveAnalysis: Boolean = true

  /**
    * Retrieve the jdbc / sql type for a given Avro data type.
    *
    * @param dt The datatype
    * @return The new JdbcType if there is an override for this DataType
    */
  def getJDBCType(dt: Schema): Option[JdbcType] = None

  /**
    * Quotes the identifier. This is used to put quotes around the identifier in case the column
    * name is a reserved keyword, or in case it contains characters that require quotes (e.g. space).
    */
  def quoteIdentifier(colName: String): String = {
    s""""$colName""""
  }

  /**
    * We rely on the database to convert a TEXT/CHAR sql type to JSON.
    * Most databases have string to json converters; this method should
    * return the correct one for the dialect.
    */
  def jsonPlaceholder: String = "?"

  /**
    * Get the SQL query that should be used to find if the given table exists. Dialects can
    * override this method to return a query that works best in a particular database.
    *
    * @param table The name of the table.
    * @return The SQL query to use for checking the table.
    */
  def getTableExistsQuery(table: String): String = {
    s"SELECT * FROM $table WHERE 1=0"
  }

  /**
    * The SQL query that should be used to discover the schema of a table. It only needs to
    * ensure that the result set has the same schema as the table, such as by calling
    * "SELECT * ...". Dialects can override this method to return a query that works best in a
    * particular database.
    *
    * @param table The name of the table.
    * @return The SQL query to use for discovering the schema.
    */
  def getSchemaQuery(table: String): String = {
    s"SELECT * FROM $table WHERE 1=0"
  }

  /**
    * Return Some[true] iff `TRUNCATE TABLE` causes cascading default.
    * Some[true] : TRUNCATE TABLE causes cascading.
    * Some[false] : TRUNCATE TABLE does not cause cascading.
    * None: The behavior of TRUNCATE TABLE is unknown (default).
    */
  def isCascadingTruncateTable(): Option[Boolean] = None

  /**
    * A default implementation for insert statements
    *
    */
  def insertStatement(table: String, schema: Schema, dbs: DbSyntax): String = {
    import scala.collection.JavaConverters._
    val columns = schema.getFields.asScala
    val cols = columns.map(c => quoteIdentifier(dbs.format(c.name))).mkString(",")
    s"INSERT INTO $table ($cols) VALUES (${parameterize(columns).mkString(",")})"
  }

  def deleteStatement(table: String, keys: Seq[Field], dbs: DbSyntax): String = {
    //guard against some rogue caller trying to delete the entire table
    assert(!keys.isEmpty, "Whoa! At least one primary key is required.")
    val colTuples = keys.map(f => s"${dbs.format(f.name)} = ?")
    s"DELETE FROM $table WHERE ${colTuples.mkString(" AND ")}"
  }

  /**
    * Returns the upsert statment for this dialect.
    * Optional operation; default implementation throws a UnsupportedOperationException
    */
  @throws[UnsupportedOperationException]
  def buildUpsert(table: String, schema: Schema, dbs: DbSyntax): String = {
    throw new UnsupportedOperationException("Upserts are not supported by this dialect.")
  }

  /**
    * Returns the upsert fields to be used in the prepared statement. These should match in size and order
    * the sql statement that gets returned from buildUpsert.
    *
    * Optional operation; default implementation throws a UnsupportedOperationException
    */
  @throws[UnsupportedOperationException]
  def upsertFields(schema: Schema): Seq[Field] = {
    throw new UnsupportedOperationException("Upserts are not supported by this dialect.")
  }

  /**
    * Convenience method that calls the underlying buildUpsertStatement method if the id exists.
    * It will add a "keys" property to the schema if the ids passed are not empty.
    */
  def upsert(table: String, schema: Schema, dbs: DbSyntax): String = {
    Option(schema.getProp("hydra.key")).map(_ => buildUpsert(table, schema, dbs))
      .getOrElse(insertStatement(table, schema, dbs))
  }

  protected def parameterize(fields: Seq[Schema.Field]): Seq[String] = {
    fields.map { c =>
      if (JdbcUtils.getJdbcType(c.schema(), this).databaseTypeDefinition == "JSON") jsonPlaceholder else "?"
    }
  }

  /*
   * Optional operation; default implementation throws a UnsupportedOperationException
    */
  @throws[UnsupportedOperationException]
  def alterTableQueries(tableName: String, missingFields: Seq[Field], dbs: DbSyntax): Seq[String] = {
    throw new UnsupportedOperationException("Alter tables are not supported by this dialect.")
  }
}

object JdbcDialects {

  /**
    * Register a dialect for use on all new matching jdbc
    * Reading an existing dialect will cause a move-to-front.
    *
    * @param dialect The new dialect.
    */
  def registerDialect(dialect: JdbcDialect): Unit = {
    dialects = dialect :: dialects.filterNot(_ == dialect)
  }

  /**
    * Unregister a dialect. Does nothing if the dialect is not registered.
    *
    * @param dialect The jdbc dialect.
    */
  def unregisterDialect(dialect: JdbcDialect): Unit = {
    dialects = dialects.filterNot(_ == dialect)
  }

  def registeredDialects: Seq[JdbcDialect] = Seq(dialects: _*)

  private[this] var dialects = List[JdbcDialect]()
  registerDialect(PostgresDialect)
  registerDialect(DB2Dialect)

  /**
    * Fetch the JdbcDialect class corresponding to a given database url.
    */
  def get(url: String): JdbcDialect = {
    val matchingDialects = dialects.filter(_.canHandle(url))
    matchingDialects.length match {
      case 0 => NoopDialect
      case 1 => matchingDialects.head
      case _ => new AggregatedDialect(matchingDialects)
    }
  }
}

/**
  * NOOP dialect object, always returning the neutral element.
  */
private object NoopDialect extends JdbcDialect {
  override def canHandle(url: String): Boolean = true

  override def buildUpsert(table: String, schema: Schema, dbs: DbSyntax): String = {
    throw new UnsupportedOperationException("Not supported.")
  }
}

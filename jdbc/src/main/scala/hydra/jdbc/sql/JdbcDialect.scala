package hydra.jdbc.sql

import org.apache.avro.Schema

/**
  * Created by alexsilva on 5/4/17.
  */
abstract class JdbcDialect extends Serializable {
  /**
    * Check if this dialect instance can handle a certain jdbc url.
    *
    * @param url the jdbc url.
    * @return True if the dialect can be applied on the given jdbc url.
    * @throws NullPointerException if the url is null.
    */
  def canHandle(url: String): Boolean


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
}


object JdbcDialects {

  /**
    * Register a dialect for use on all new matching jdbc `org.apache.spark.sql.DataFrame`.
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
}

package hydra.sql

/**
  * Created by alexsilva on 7/11/17.
  */
class NoSuchSchemaException(schema: String) extends AnalysisException(s"Schema '$schema' not found")

class NoSuchTableException(schema: String, table: String)
  extends AnalysisException(s"Table or view '$table' not found in schema '$schema'")
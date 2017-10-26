package hydra.sql

/**
  * Created by alexsilva on 7/11/17.
  */
class DatabaseAlreadyExistsException(db: String)
  extends AnalysisException(s"Database '$db' already exists")

class TableAlreadyExistsException(db: String, table: String)
  extends AnalysisException(s"Table or view '$table' already exists in database '$db'")

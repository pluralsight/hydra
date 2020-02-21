package hydra.sql

import java.sql.SQLType

/**
  * Created by alexsilva on 5/4/17.
  */
case class JdbcType(databaseTypeDefinition: String, targetSqlType: SQLType)

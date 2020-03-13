package hydra.sql

import com.typesafe.config.Config
import hydra.common.config.ConfigSupport._
import hydra.common.reflect.ReflectionUtils

case class JdbcWriterSettings(private val config: Config) {

  /**
    * The db syntax implementation to use.
    */
  val dbSyntax: DbSyntax = config
    .getStringOpt("db.syntax")
    .map(Class.forName)
    .map(ReflectionUtils.getObjectInstance(_).asInstanceOf[DbSyntax])
    .getOrElse(UnderscoreSyntax)

  /**
    * The maximum number of times to retry on errors before failing.
    */
  val maxRetries: Int = config.getIntOpt("max.retries").getOrElse(10)

  /**
    * Number of records to batch together for insertion into the destination table.
    */
  val batchSize: Int = config.getIntOpt("batch.size").getOrElse(3000)

  /**
    * Whether to automatically the destination table based on record schema.
    */
  val autoEvolve: Boolean = config.getBooleanOpt("auto.evolve").getOrElse(true)

}

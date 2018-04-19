package hydra.sql

import com.typesafe.config.Config
import configs.syntax._
import hydra.common.reflect.ReflectionUtils

case class JdbcWriterSettings(private val config: Config) {
  /**
    * The db syntax implementation to use.
    */
  val dbSyntax: DbSyntax = config.get[String]("db.syntax")
    .map(Class.forName).map(ReflectionUtils.getObjectInstance(_).asInstanceOf[DbSyntax])
    .valueOrElse(UnderscoreSyntax)

  /**
    * The maximum number of times to retry on errors before failing.
    */
  val maxRetries: Int = config.get[Int]("max.retries").valueOrElse(10)

  /**
    * Number of records to batch together for insertion into the destination table.
    */
  val batchSize: Int = config.get[Int]("batch.size").valueOrElse(3000)

  /**
    * Whether to automatically the destination table based on record schema.
    */
  val autoEvolve: Boolean = config.get[Boolean]("auto.evolve").valueOrElse(true)

}

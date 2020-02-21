package hydra.sql

/**
  * Created by alexsilva on 7/11/17.
  */

sealed trait IdentifierWithSchema {
  val identifier: String

  def schema: Option[String]
}

case class TableIdentifier(
    table: String,
    database: Option[String],
    schema: Option[String]
) extends IdentifierWithSchema {

  override val identifier: String = table

}

object TableIdentifier {

  def apply(tableName: String): TableIdentifier =
    TableIdentifier(tableName, None, None)
}

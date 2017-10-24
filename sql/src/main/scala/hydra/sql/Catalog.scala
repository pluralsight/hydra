package hydra.sql

import scala.util.Try

abstract class Catalog {

  def createTable(table: Table):Boolean

  def createOrAlterTable(tableDesc: Table): Boolean

  def createSchema(schema: String): Boolean

  def tableExists(tableIdentifier: TableIdentifier): Boolean

  def schemaExists(schema: String): Boolean

  def getTableMetadata(tableIdentifier: TableIdentifier): Try[DbTable]

}

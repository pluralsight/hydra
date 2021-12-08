package hydra.kafka.algebras


trait SnowflakeAlgebra[F[_]] {
  getCurrentTableNames: F[List[String]]
  getHistoricalTableNames: F[List[String]]
  getAllTableNames: F[List[List[String]]]
}

object SnowflakeAlgebra {

}

private case class SnowflakeStorageFacade(tablesMap: Map[String, List[String]]) {
  def updateTableNamesList(tableType: String, tableNames: List[String]): SnowflakeStorageFacade = {
    this.copy(this.tablesMap + (tableType, tableNames))
  }
}

private object SnowflakeStorageFacade {
  def empty: SnowflakeStorageFacade = SnowflakeStorageFacade(Map("current" -> List[String](), "historical" -> List[String]()))
}
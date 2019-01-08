package hydra.auth.persistence

import java.sql.Timestamp

import hydra.core.persistence.PersistenceDelegate
import org.joda.time.DateTime

trait RepositoryModels {
  val persistenceDelegate: PersistenceDelegate

  import persistenceDelegate.profile.api._

  implicit def dateTime = MappedColumnType.base[DateTime, Timestamp](
    dt => new Timestamp(dt.getMillis),
    ts => new DateTime(ts))

  type TokenType = (Int, DateTime, DateTime, String, Int)

  class TokenTable(tag: Tag) extends Table[TokenType](tag, Some("ingest"), "token") {
    def id = column[Int]("id", O.PrimaryKey, O.AutoInc)

    def createdDate = column[DateTime]("created_date")

    def modifiedDate = column[DateTime]("modified_date")

    def token = column[String]("token")

    def groupId = column[Int]("group_id")

    def * = (id, createdDate, modifiedDate, token, groupId)

    def groupConstraint = foreignKey("token_group_fk", groupId, groupTable)(_.id)
  }

  lazy val tokenTable = TableQuery[TokenTable]

  type GroupType = (Int, String, DateTime, DateTime)

  class GroupTable(tag: Tag) extends Table[GroupType](tag, Some("ingest"), "group") {
    def id = column[Int]("id", O.PrimaryKey, O.AutoInc)

    def name = column[String]("name")

    def createdDate = column[DateTime]("created_date")

    def modifiedDate = column[DateTime]("modified_date")

    def * = (id, name, createdDate, modifiedDate)
  }

  lazy val groupTable = TableQuery[GroupTable]

  type ResourceType = (Int, String, String, Int)

  class ResourceTable(tag: Tag) extends Table[ResourceType](tag, Some("ingest"), "resource") {
    def id = column[Int]("id", O.PrimaryKey, O.AutoInc)

    def name = column[String]("name")

    def resourceType = column[String]("resource_type")

    def groupId = column[Int]("group_id")

    def * = (id, name, resourceType, groupId)

    def groupConstraint = foreignKey("resources_groups_fk", groupId, groupTable)(_.id)
  }

  lazy val resourceTable = TableQuery[ResourceTable]
}

object RepositoryModels {
  // Corresponding case classes
  case class Token(id: Int, createdDate: DateTime, modifiedDate: DateTime, token: String,
                   groupId: Int)

  case class Group(id: Int, name: String, createdDate: DateTime, modifiedDate: DateTime)

  case class Resource(id: Int, name: String, resourceType: String, groupId: Int)
}

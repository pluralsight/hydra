package hydra.auth.persistence

import java.sql.Timestamp

import org.joda.time.DateTime
import slick.jdbc.PostgresProfile.api._

object RepositoryModels {
  implicit def dateTime = MappedColumnType.base[DateTime, Timestamp](
    dt => new Timestamp(dt.getMillis),
    ts => new DateTime(ts))

  type Token = (Int, DateTime, DateTime, String, Int)

  class Tokens(tag: Tag) extends Table[Token](tag, "tokens") {
    def id = column[Int]("id", O.PrimaryKey, O.AutoInc)

    def createdDate = column[DateTime]("created_date")

    def modifiedDate = column[DateTime]("modified_date")

    def token = column[String]("token")

    def groupId = column[Int]("group_id")

    def * = (id, createdDate, modifiedDate, token, groupId) <> (Token)

    def groupConstraint = foreignKey("tokens_groups_fk", groupId, groups)(_.id)
  }

  lazy val tokens = TableQuery[Tokens]

  type Group = (Int, String, DateTime, DateTime)

  class Groups(tag: Tag) extends Table[Group](tag, "groups") {
    def id = column[Int]("id", O.PrimaryKey, O.AutoInc)

    def name = column[String]("name")

    def createdDate = column[DateTime]("created_date")

    def modifiedDate = column[DateTime]("modified_date")

    def * = (id, name, createdDate, modifiedDate)
  }

  lazy val groups = TableQuery[Groups]

  type Resource = (Int, String, Int)

  class Resources(tag: Tag) extends Table[Resource](tag, "resources") {
    def id = column[Int]("id", O.PrimaryKey, O.AutoInc)

    def name = column[String]("name")

    def groupId = column[Int]("group_id")

    def * = (id, name, groupId)

    def groupConstraint = foreignKey("resources_groups_fk", groupId, groups)(_.id)
  }

  // Corresponding case classes
  case class Token(id: Int, createdDate: DateTime, modifiedDate: DateTime, token: String,
                   groupId: String)

  case class Group(id: Int, name: String, createdDate: DateTime, modifiedDate: DateTime)

  case class Resource(id: Int, name: String, groupId: Int)
}

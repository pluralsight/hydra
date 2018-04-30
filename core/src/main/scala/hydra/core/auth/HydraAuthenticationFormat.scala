package hydra.core.auth

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import spray.json.DefaultJsonProtocol

trait HydraAuthenticationFormat extends DefaultJsonProtocol with SprayJsonSupport {
  case class Role(id: String, roleName: String, description: String)

  case class User(id: Long, username: String, password: String, firstName: String, lastName: String,
                  roles: Seq[Role])

  case class AuthPayload(users: Seq[User])

  implicit val roleFmt = jsonFormat3(Role)

  implicit val userFmt = jsonFormat6(User)
}

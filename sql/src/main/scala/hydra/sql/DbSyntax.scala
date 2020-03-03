package hydra.sql

import com.google.common.base.CaseFormat

/**
  * Created by alexsilva on 7/12/17.
  */
trait DbSyntax {
  def format(identifier: String): String
}

object UnderscoreSyntax extends DbSyntax {

  override def format(identifier: String): String = {
    CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, identifier)
  }
}

object NoOpSyntax extends DbSyntax {

  override def format(identifier: String): String = identifier

}

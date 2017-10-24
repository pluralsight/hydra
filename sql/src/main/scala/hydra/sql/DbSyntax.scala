package hydra.sql

import com.google.common.base.CaseFormat

/**
  * Created by alexsilva on 7/12/17.
  */
trait DbSyntax {
  def format(identifier: String): String
}

object UnderscoreSyntax extends DbSyntax {
  val converter = CaseFormat.UPPER_CAMEL.converterTo(CaseFormat.LOWER_UNDERSCORE)

  override def format(identifier: String): String = {
    converter.convert(identifier)
  }
}

object NoOpSyntax extends DbSyntax {

  override def format(identifier: String): String = identifier

}

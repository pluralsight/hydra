package hydra.core.http.security

import com.amazonaws.auth.policy.Action

object AwsIamPolicyAction {

  sealed trait KafkaAction extends Action {
    override def getActionName: String = s"kafka-cluster:${this.toString}"
  }

  object KafkaAction {
    case object CreateTopic extends KafkaAction

    case object DeleteTopic extends KafkaAction

    case object ReadData extends KafkaAction

    case object WriteData extends KafkaAction

    case class CustomAction(action: String) extends KafkaAction {
      override def toString: String = action
    }
  }
}

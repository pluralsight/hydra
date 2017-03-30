package hydra.kafka.model

/**
  * Created by alexsilva on 3/30/17.
  */
case class TopicMetadata(name: String, description: String, organization: String, team: String, contact: String,
                         schema: String, tags: Seq[String] = Seq.empty)

package hydra.common.auth

import akka.actor.Status.Failure
import akka.actor.{ActorSystem, Props}
import akka.testkit.{TestKit, TestProbe}
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.concurrent.Future

class TokenActorSpec extends TestKit(ActorSystem("token-actor-spec"))
  with FlatSpecLike
  with Matchers
  with BeforeAndAfterAll
  with MockFactory {

  implicit val ec = system.dispatcher

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "A TokenActor" should "should get token from repo if not in cache" in {
    val tokenInfo = TokenGenerator.generateTokenInfo

    val listener = TestProbe()

    val repoStub = stub[ITokenInfoRepository]

    (repoStub.getByToken _)
      .when(tokenInfo.token)
      .returning(Future.successful(tokenInfo))

    val tokenActor = system.actorOf(Props(classOf[TokenActor], repoStub))

    // call to insert into cache
    tokenActor.tell(GetToken(tokenInfo.token), listener.ref)

    (repoStub.getByToken _)
      .verify(tokenInfo.token)
      .once

    listener.expectMsg(tokenInfo)
  }
  "A TokenActor" should "return a failed future if a token can't be found in the cache or db" in {
    val tokenInfo = TokenGenerator.generateTokenInfo

    val listener = TestProbe()

    val repoStub = stub[ITokenInfoRepository]

    (repoStub.getByToken _)
      .when(*)
      .returning(Future.failed(new RuntimeException()))

    val tokenActor = system.actorOf(Props(classOf[TokenActor], repoStub))

    // call to insert into cache
    tokenActor.tell(GetToken(tokenInfo.token), listener.ref)

    (repoStub.getByToken _)
      .verify(tokenInfo.token)
      .once

    listener.expectMsgPF() {
      case Failure(ex) => ex.isInstanceOf[RuntimeException]
    }
  }
}
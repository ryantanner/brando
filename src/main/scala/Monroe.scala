package brando

import akka.actor.{ Actor, ActorRef, Props, Status, ActorSystem }
import akka.io.{ IO, Tcp }
import akka.util.{ ByteString, Timeout }
import akka.pattern.{ ask, pipe }
import akka.channels._
import akka.event.Logging
import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration._

class Monroe(
    brando: ActorRef) extends Actor {

  val log = Logging(context.system, this)

  def receive = {
    case cmd: TypedRequest =>
      brando forward Commands.toRequest(cmd)
  }

}

object Monroe {

  def apply(implicit system: ActorSystem) = {
    new ChannelRef[(TypedRequest, Option[StatusReply]) :+: TNil](
      system.actorOf(Props(classOf[Monroe], system.actorOf(Brando())))
    )
  }

}

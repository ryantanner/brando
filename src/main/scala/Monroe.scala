package brando

import akka.actor.{ Actor, ActorRef, Props, Status }
import akka.io.{ IO, Tcp }
import akka.util.{ ByteString, Timeout }
import akka.pattern.{ ask, pipe }
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


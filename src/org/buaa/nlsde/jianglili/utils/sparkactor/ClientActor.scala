package org.buaa.nlsde.jianglili.utils.sparkactor

import akka.actor.{Actor, ActorRef, ActorSelection, ActorSystem, Props}
import com.typesafe.config.ConfigFactory

/**
  * Created by jianglili on 2017/2/27.
  */
class ClientActor extends Actor {

  var serverActor: ActorSelection = null

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    serverActor = context.actorSelection("akka.tcp://myServerSys@127.0.0.1:9999/user/server")
  }

  override def receive: Receive = {
    case msg: String => {
      if (msg.startsWith("send:")) {
        println(msg.split(":")(1)+" will send to server")
        serverActor.tell(msg.split(":")(1),self)
      } else {
        println("Client:receive from server: " + msg)
      }
    }
    case _ => println("ERROR MESSAGE TYPE... ")
  }
}

object Client {
  def main(args: Array[String]): Unit = {
    val clientSystem = ActorSystem("myClientSys", ConfigFactory.parseString(
      """
  akka {
  actor {
  provider = "akka.remote.RemoteActorRefProvider"
  }
  }
      """))

    val clientActor = clientSystem.actorOf(Props(classOf[ClientActor]))

    clientActor.tell("send:test test test",ActorRef.noSender);
    clientActor.tell("send:test1 test test",ActorRef.noSender);
    clientActor.tell("send:test2 test test",ActorRef.noSender);

  }
}
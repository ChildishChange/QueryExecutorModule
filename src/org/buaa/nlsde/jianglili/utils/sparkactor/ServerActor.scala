package org.buaa.nlsde.jianglili.utils.sparkactor

/**
  * Created by jianglili on 2017/2/27.
  */
import akka.actor.{ActorRef, _}
import com.typesafe.config.ConfigFactory
class ServerActor extends Actor {
  override def receive: Receive = {
    case msg: String => {
      println("Server:Got message:" + msg)
      sender.tell("Server:" + msg,ActorRef.noSender)
    }
    case _ => println("ERROR MESSAGE TYPE... ")
  }
}

object Server {
  def main(args: Array[String]): Unit = {
    val serverSystem = ActorSystem("myServerSys", ConfigFactory.parseString(
      """
  akka {
  actor {
  provider = "akka.remote.RemoteActorRefProvider"
  }
  remote {
  enabled-transports = ["akka.remote.netty.tcp"]
  netty.tcp {
  hostname = "127.0.0.1"
  port = 9999
  }
  }
  } """))

    val serverActor=serverSystem.actorOf(Props(classOf[ServerActor]), "server")

  }
}
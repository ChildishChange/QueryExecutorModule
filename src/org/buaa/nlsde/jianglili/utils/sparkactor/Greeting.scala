package org.buaa.nlsde.jianglili.utils.sparkactor

import akka.actor.{Actor, ActorLogging, ActorSystem, Props}

/**
  * Created by jianglili on 2017/2/27.
  */
case class Greeting(who: String)
class GreetingActor extends Actor with ActorLogging {
  def receive = {
    case Greeting(who) ⇒ log.info("Hello " + who)
  }
}
object Main{
  def main(args: Array[String]) {
    val system = ActorSystem("MySystem")
    val greeter = system.actorOf(Props[GreetingActor], name = "greeter")
    greeter ! Greeting("Charlie Parker")
  }
}


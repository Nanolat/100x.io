package io.x100

import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.Props
import scala.concurrent.ExecutionContext.Implicits.global

case class QueryMessage(query:String)

class QueryCompilingActor extends Actor {
  def receive = {
    case r : QueryMessage => {
    }
  }
}

trait QueryPlan

class QueryExecutingActor extends Actor {
  def receive = {
    case r : QueryPlan => {
    }
  }
}

case class TermJoinRequest(term1 : Long, term2 : Long)

class TermJoiningActor extends Actor {
  def receive = {
    case r : TermJoinRequest => {

    }
  }
}

case class TermScanRequest(term : Long)

class TermScanningActor extends Actor {
  def receive = {
    case r : TermScanRequest => {

    }
  }
}

case class SnippetRequest(terms : Array[Long], documentId : Long)

class SnippetGeneratingActor extends Actor {
  def receive = {
    case r : SnippetRequest => {

    }
  }
}

object Actors {
  val system = ActorSystem("momdero")
  val queryCompiler = system.actorOf(Props[QueryCompilingActor], name= "query-compiling-actor")
  val queryExecutor = system.actorOf(Props[QueryExecutingActor], name= "query-executing-actor")
  val termJoiner = system.actorOf(Props[TermJoiningActor], name= "posting-joining-actor")
  val termScanner = system.actorOf(Props[TermScanningActor], name= "posting-joining-actor")
  val snippetGenerator = system.actorOf(Props[SnippetGeneratingActor], name = "snippet-generating-actor")
}
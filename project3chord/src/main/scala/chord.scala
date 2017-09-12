import akka.actor.ActorSystem
import scala.concurrent.duration._
import java.math.BigInteger
import java.security.MessageDigest
import scala.collection.mutable.ArrayBuffer
import akka.actor.{ActorRef, Props, ActorLogging, Actor}

case object ChordInitialize     //initialize the chord
case object SendRequests   //system send request to chord
case class NodeInitialize(interval: (Int, Int), table: Array[(Int, (Int, Int), ActorRef)])
case class NodeRequest(key: Int, sender: Int, hops: Int)                 //sender actor sent request to successor
case class NodeResponse(key: Int, sender: Int, finder: Int, hops: Int)   // sender actor sent response to chord

object project3 extends App {

  val numNodes: Int = Integer.parseInt(args(0))
  val numRequests: Int = Integer.parseInt(args(1))
  val system = ActorSystem("Chord")
  val chord = system.actorOf(Props(new Chord()), "CHORD")
  chord ! ChordInitialize

  def propsNodecheck(identifier: Int): Props = Props(new NodeCheck(identifier))

  // Create cancellable scheduler to send requests every 1 second ,and the delay is 2s
  import system.dispatcher
  val SchedulerRequest = system.scheduler.schedule(2 seconds , 1 seconds , chord, SendRequests)
  system.awaitTermination()
}


class Chord() extends Actor with ActorLogging {

  val m: Int = 16  // identifier bits
  val modBase: Int = 1 << m  // 2 ^ m
  val modBaseBigInt: BigInteger = new BigInteger(modBase.toString)

  val peers = new ArrayBuffer[Node]()           // array of all nodes actors
  var idSet = Set[Int]()                        // set of identifiers currently in use
  var NodeIndex = 0                            // count the node index used
  var RequestTimes = 0                         // count request times
  var TotalRequestNum = 0                          // count total requests sent numbers
  var TotalResponseNum = 0                         // count total responses received
  var sumHops = 0                               // total sum of hops

  class Node(id: Int, actor: ActorRef) {
    val identifier: Int = id
    var interval: (Int, Int) = null             // Interval from start to identifier
    val actorRef: ActorRef = actor
    val table = new Array[(Int, (Int, Int), ActorRef)](m)  // node id, position interval, actor
  }

  // Generate a new unique identifier.
  def getSHAValue(text: String): Int = {
    val digest: MessageDigest = MessageDigest.getInstance("SHA-256")
    val hash: Array[Byte] = digest.digest(text.getBytes("UTF-8"))
    val bigInt: BigInteger = new BigInteger(1, hash)
    val ID = bigInt.mod(modBaseBigInt).intValue()
    return ID
  }


  // Generate a new unique identifier.

  def ggetId(): Int = {
    var id = getSHAValue("Node" + NodeIndex)
    NodeIndex += 1
    while (idSet(id)) {
      id = getSHAValue("Node" + NodeIndex)
      NodeIndex += 1
    }
     return id
  }

  //  Initialize the circle network.
  def getCircle(): Unit = {
    getIntervals()
    getTables()
    for (node <- peers)
      node.actorRef ! NodeInitialize(node.interval, node.table)                 // generate all the nodes
  }


  // Generate the key intervals for each node.

  def getIntervals(): Unit = {
    var start: Int = (peers.last.identifier + 1) % modBase
    for (node <- peers) {
      val end = node.identifier
      node.interval = (start, end)
      start = end + 1
    }
  }

  // Generate the finger interval tables for each node.
  def getTables(): Unit = {
    for (node <- peers) {
      var base = 1
      for (i <- 0 until m) {
        val start: Int = (node.identifier + base) % modBase
        base *= 2
        val end: Int = (node.identifier + base - 1) % modBase

        for (k <- peers) {
          if (CheckInInterval(start, k.interval))
            node.table(i) = (k.identifier, (start, end), k.actorRef)    //???
        }
      }
    }
  }


  def CheckInInterval(position: Int, interval: (Int, Int)): Boolean = {
    if (interval._1 > interval._2)
      return position >= interval._1 || position <= interval._2
    position >= interval._1 && position <= interval._2
  }


  // Message handling for chord actor.
  def receive = {
    case ChordInitialize =>
      println("Chord system information:"+ project3.numNodes + "nodes," + project3.numRequests + "requests, m of the identifier circle is 16.")
      val ids = new Array[Int](project3.numNodes)     //array of nodes
      for (i <- ids.indices) {
        ids(i) = ggetId()    //set of identifiers currently in use
        idSet += ids(i)
      }
      val sortedIds = ids.sorted      // sorted node ids
      for (id <- sortedIds)
        peers += new Node(id, context.system.actorOf(project3.propsNodecheck(id), "Node" + id))      // generate all the nodes
      getCircle()

    // Send 1 request with random key to each node
    case SendRequests =>
      RequestTimes += 1
        println("Sending the " + RequestTimes + "time of requests. ")
        for (node <- peers) {
          val key = scala.util.Random.nextInt(modBase) // generate  random number  to send request
          node.actorRef ! NodeRequest(key, node.identifier, 0)
          println("Node" + node.identifier + " sends request with the key:" + key)
        }
      TotalRequestNum += peers.length
        if (RequestTimes >= project3.numRequests)
         project3.SchedulerRequest.cancel()


    // Receive response from a node
    case NodeResponse(key: Int, sender: Int, finder: Int, hops: Int) =>
      TotalResponseNum += 1
      sumHops += hops
      println("Node:"+ finder + "find the key : "+ key + " frome the sender " + sender +"using " + hops + "hops !" )
      // Finished all requests and all messages received
      if (TotalResponseNum >= TotalRequestNum && project3.SchedulerRequest.isCancelled) {
        val averageHops = 1.0 * sumHops / TotalResponseNum
        println("Finished"+ TotalResponseNum + "messages, the average hops per messages is :"+ averageHops )
        context.system.shutdown()
      }
  }
}


class NodeCheck(identifier: Int) extends Actor with ActorLogging {
  var KeyInterval: (Int, Int) = null  // inclusive interval of keys covered by this node
  var Fingerable: Array[(Int, (Int, Int), ActorRef)] = null  // node id, position interval, actor

  // Check if a position is within the interval
  def CheckInInterval(position: Int, interval: (Int, Int)): Boolean = {
    if (interval._1 > interval._2)
      return position >= interval._1 || position <= interval._2
    position >= interval._1 && position <= interval._2
  }

  // Find the next hop from the finger table.
  def nextHop(key: Int): ActorRef = {
    for (tuple <- Fingerable.reverse) {
      if (CheckInInterval(key, tuple._2))
        return tuple._3
    }
    null
  }

  def receive = {
    case NodeInitialize(interval: (Int, Int), table: Array[(Int, (Int, Int), ActorRef)]) =>
      KeyInterval = interval
      Fingerable = table

    // Receive request from master or another node
    case NodeRequest(key: Int, sender: Int, hops: Int) =>
      if (CheckInInterval(key, KeyInterval))
        project3.chord ! NodeResponse(key, sender, identifier, hops + 1)
      else
        nextHop(key) ! NodeRequest(key, sender, hops + 1)
      println("Still Send request from  "+ sender + "  to  " + nextHop(key)  )
  }
}

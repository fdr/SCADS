package edu.berkeley.cs.scads.util

import scala.collection.mutable.{HashMap}

import edu.berkeley.cs.avro.marker.AvroRecord
import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.mapreduce.{Mapper, MapperContext,
                                        Reducer, ReducerContext}
import edu.berkeley.cs.scads.storage._

import twitter._

case class LongSeqRec(var f1: List[Long]) extends AvroRecord
case class RetweetGraphRec(var id: Long,
                           var depth: Long,
                           var size: Long,
                           var children: List[RetweetGraphRec])
    extends AvroRecord

class RetweetMapper extends Mapper {
  @inline def singleMap(key: HashLongRec, value: StringRec, context: MapperContext) = {
    val id = key.f1
    val tweet = value.f1
    val tweetMap = JsonParser.parseJson(tweet)

    // REPLY outputs
/*
    tweetMap.get("in_reply_to_status_id") match {
      case None =>
      case Some(parent_id) =>
        if (parent_id != null) {
          val longVal = try {
            parent_id.asInstanceOf[Int].toLong
          } catch {
            case _ => parent_id.asInstanceOf[BigInt].longValue
          }
          context.collect(LongRec(longVal), LongRec(id))
        }
    }
*/
    // RETWEET outputs

    tweetMap.get("retweeted_status-id") match {
      case None => 
      case Some(parent_id) =>
        val longVal = try {
          parent_id.asInstanceOf[Int].toLong
        } catch {
          case _ => parent_id.asInstanceOf[BigInt].longValue
        }
        context.collect(LongRec(longVal), LongRec(id))
    }

  }
  def map(data: Seq[(AvroRecord, AvroRecord)], context: MapperContext) = {
    data.foreach {
      case (key, value) => singleMap(key.asInstanceOf[HashLongRec],
                                     value.asInstanceOf[StringRec], context)
    }
  }
}
class RetweetCombiner extends Reducer {
  def reduce(key: AvroRecord, values: Seq[AvroRecord],
      context: ReducerContext): Unit = {
    val idRec = key.asInstanceOf[LongRec]
    val list = values.map(x => x.asInstanceOf[LongRec].f1)
        .foldLeft(List[Long]())((l, i) => i::l)
    context.collect(idRec, LongSeqRec(list))
  }
}
class RetweetReducer extends Reducer {
  def reduce(key: AvroRecord, values: Seq[AvroRecord],
      context: ReducerContext): Unit = {
    val idRec = key.asInstanceOf[LongRec]
    val list = values.map(x => x.asInstanceOf[LongSeqRec].f1)
        .foldLeft(List[Long]())((l, i) => i:::l)
    context.collect(idRec, LongSeqRec(list))
  }
}

class RetweetGraphMapper extends Mapper {
  def getTree(id: Long, context: MapperContext,
              childrenOpt: Option[List[Long]]):RetweetGraphRec = {
    val children = childrenOpt match {
      case None => 
        context.getKey[LongRec, LongSeqRec]("retweetIndex", LongRec(id)) match {
          case None => List[Long]()
          case Some(value) => value.f1
        }
      case Some(list) => list
    }

    def max(x: Long, y: Long) = if (x < y) y else x
    val tree = children.map(getTree(_, context, None))
    val (depth, size) = tree.foldLeft((0L, 1L))(
      (info, x) => (max(x.depth, info._1), info._2 + x.size))

    RetweetGraphRec(id, depth + 1, size, tree)
  }

  @inline def singleMap(key: LongRec, value: LongSeqRec, context: MapperContext) = {
    val id = key.f1
    val children = value.f1

    val isTopMost = context.getKey[HashLongRec, StringRec](
        "tweets", HashLongRec(Hash.hashMurmur2(id.toString.toArray.map(_.toByte)), id)) match {
//      case None => false
      case None => true
      case Some(t) => 
        val tweetMap = JsonParser.parseJson(t.f1)
        tweetMap.get("retweeted_status-id") match {
//        tweetMap.get("in_reply_to_status_id") match {
          case None => true
          case Some(parent_id) => false
//          case Some(parent_id) => if (parent_id == null) true else false
        }
    }
    if (isTopMost) {
      // This is the top most tweet of a retweet graph.
      val tree = getTree(id, context, Some(children))
      if (tree.depth > 1)
        context.collect(LongRec(id), tree)
    }
  }
  def map(data: Seq[(AvroRecord, AvroRecord)], context: MapperContext) = {
    data.foreach {
      case (key, value) => singleMap(key.asInstanceOf[LongRec],
                                     value.asInstanceOf[LongSeqRec], context)
    }
  }
}
class RetweetGraphReducer extends Reducer {
  def reduce(key: AvroRecord, values: Seq[AvroRecord],
      context: ReducerContext): Unit = {
    context.collect(key, values.head)
  }
}



class TestTwitterMapper {
  def run() {
    val client = TestScalaEngine.newScadsCluster(4)
    val storageServer = client.getAvailableServers
    val numServers = storageServer.length

    val partitionList = None :: (1 until numServers).map(
            x => Some(HashLongRec(0xffffffffL / numServers * x, 0))
        ).toList zip storageServer.map(List(_))
    val ns1 = client.createNamespace[HashLongRec, StringRec](
        "tweets",
        partitionList)
//        List((None, List(storageServer(0))),
//             (LongRec(7914403051L), List(storageServer(1))),
//             (LongRec(7922702502L), List(storageServer(2))),
//             (LongRec(7931001952L), List(storageServer(3)))))

//        List((None, List(storageServer(0)))))
//7914403051
//7922702502
//7931001953

    println("*****************************")
    println("********Loading Data*********")
    println("*****************************")

    // min: 7906103600L , max: 7939301404L
    val filenames =
      "../../data_twitter/raw.0001.txt.gz" ::
//      "../../data_twitter/raw.bad.txt.gz" ::
/*
      "../../data_twitter/raw.0014.txt.gz" ::
      "../../data_twitter/raw.0015.txt.gz" ::
      "../../data_twitter/raw.0016.txt.gz" ::
      "../../data_twitter/raw.0017.txt.gz" ::
      "../../data_twitter/raw.0018.txt.gz" ::
      "../../data_twitter/raw.0019.txt.gz" ::
      "../../data_twitter/raw.0020.txt.gz" ::
      "../../data_twitter/raw.0021.txt.gz" ::
      "../../data_twitter/raw.0022.txt.gz" ::
      "../../data_twitter/raw.0023.txt.gz" ::
      "../../data_twitter/raw.0024.txt.gz" ::
      "../../data_twitter/raw.0025.txt.gz" ::
*/
      List()

    var start_ms = System.currentTimeMillis()
    var total_tweets = 0
    filenames.foreach(f => total_tweets += TwitterLoader.loadFile(f, ns1)._1)
    var elapsed_time_ms = System.currentTimeMillis() - start_ms;
    println("\ntotal elapsed time (sec): " + elapsed_time_ms / 1000.0)
    println("total tweets: " + total_tweets)
    // dumpDistribution fails for a strange comm reason.
    // CRI [20101202-17:11:15.087] comm: Got message for an unknown service: {"num": 837}
    ns1.dumpDistribution

    // Create output namespace
    var nsOutput = client.createNamespace[LongRec, LongSeqRec](
        "retweetIndex",
        List((None, List(storageServer(0))),
             (LongRec(7914403051L), List(storageServer(1))),
             (LongRec(7922702502L), List(storageServer(2))),
             (LongRec(7931001952L), List(storageServer(3)))))

    // Execute map/reduce
    start_ms = System.currentTimeMillis()
    val ns3 = client.getNamespace[HashLongRec, StringRec]("tweets")
    ns3.executeMapReduce[LongRec, LongSeqRec](
        None, None, classOf[RetweetMapper],
        Some(classOf[RetweetCombiner]), classOf[RetweetReducer],
        "retweetIndex", true)
    elapsed_time_ms = System.currentTimeMillis() - start_ms;

    println("*****************************")
    println("********Final Result*********")
    println("*****************************")
//    nsOutput.getRange(None, None).foreach(t => println(t))
    nsOutput.dumpDistribution
    println

    println("\ntotal elapsed time (sec): " + elapsed_time_ms / 1000.0)
    println




    // Create output namespace
    val nsOutput2 = client.createNamespace[LongRec, RetweetGraphRec](
        "retweetGraph",
        List((None, List(storageServer(0))),
             (LongRec(7914403051L), List(storageServer(1))),
             (LongRec(7922702502L), List(storageServer(2))),
             (LongRec(7931001952L), List(storageServer(3)))))

    start_ms = System.currentTimeMillis()
    val ns4 = client.getNamespace[LongRec, LongSeqRec]("retweetIndex")
    ns4.executeMapReduce[LongRec, RetweetGraphRec](
        None, None, classOf[RetweetGraphMapper],
        None, classOf[RetweetGraphReducer],
        "retweetGraph", true)
    elapsed_time_ms = System.currentTimeMillis() - start_ms;

    println("*****************************")
    println("********Final Result*********")
    println("*****************************")
    nsOutput2.getRange(None, None).foreach(t => println(t))
    nsOutput2.dumpDistribution
    println

    println("\ntotal elapsed time (sec): " + elapsed_time_ms / 1000.0)
    println

  }
}

object TestTwitterMapper {
  def main(args: Array[String]) {
    val test = new TestTwitterMapper()
    test.run()
    println("Exiting...")
    System.exit(0)
  }
}
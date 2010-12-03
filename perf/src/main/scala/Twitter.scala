package edu.berkeley.cs.scads.perf
package twitter

import deploylib._
import deploylib.mesos._
import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.config._
import edu.berkeley.cs.scads.storage._
import edu.berkeley.cs.scads.util._
import edu.berkeley.cs.avro.runtime._
import edu.berkeley.cs.avro.marker._

import org.apache.zookeeper.CreateMode

case class DataLoader(var numServers: Int, var numLoaders: Int) extends DataLoadingAvroClient with AvroRecord {
  def run(clusterRoot: ZooKeeperProxy#ZooKeeperNode): Unit = {
    val coordination = clusterRoot.getOrCreate("coordination/loaders")
    val cluster = new ExperimentalScadsCluster(clusterRoot)

    val clientId = coordination.registerAndAwait("clientsStart", numLoaders)

    val servers = cluster.getAvailableServers
    println(servers)

    if (clientId == 0) {
      cluster.blockUntilReady(numServers)

      val partitionList = None :: (1 until numServers).map(
              x => Some(HashLongRec(0xffffffffL / numServers * x, 0))
              ).toList zip servers.map(List(_)) 

      println(partitionList)

      cluster.createNamespace[HashLongRec, StringRec]("tweets", partitionList)

      // create namespaces to store clientId to min/max id.
      cluster.getNamespace[IntRec, LongRec]("tweetIdMin")
      cluster.getNamespace[IntRec, LongRec]("tweetIdMax")
    }

    coordination.registerAndAwait("startWrite", numLoaders)
    val ns = cluster.getNamespace[HashLongRec, StringRec]("tweets")

    val nsMin = cluster.getNamespace[IntRec, LongRec]("tweetIdMin")
    val nsMax = cluster.getNamespace[IntRec, LongRec]("tweetIdMax")

    // LARGE files start from 2428
    // smallest file is 1
    val startFile = 5
    val numFilesToLoad = 1
    val filenameBase = "/work/marmbrus/twitter/ec2/"


    var minId:Long = Long.MaxValue
    var maxId:Long = 0

    (0 until numFilesToLoad).view.map(
        numLoaders*_ + startFile + clientId).filter(
            _ < (startFile + numFilesToLoad)).foreach(num => {
      val filename = filenameBase + "raw." + "%04d".format(num) + ".txt.gz"
      logger.info("Loader: " + clientId + ", loading file: " + filename)
      val loadStats = twitter.TwitterLoader.loadFile(filename, ns)
      println("tweets: " + loadStats._1 + ", minId: " + loadStats._2 + ", maxId: " + loadStats._3)
      if (minId < loadStats._1)
        minId = loadStats._1
      if (maxId > loadStats._2)
        maxId = loadStats._2
    })

    nsMin.put(IntRec(clientId), LongRec(minId))
    nsMax.put(IntRec(clientId), LongRec(maxId))

    coordination.registerAndAwait("endWrite", numLoaders)

    if (clientId == 0) {
      clusterRoot.createChild("clusterReady", data = this.toJson.getBytes)
    }
  }
}

case class RetweetMRClient(var numClients: Int) extends ReplicatedAvroClient with AvroRecord {

  def run(clusterRoot: ZooKeeperProxy#ZooKeeperNode): Unit = {

    val coordination = clusterRoot.getOrCreate("coordination/clients")

    numClients = 1

    val clientId = coordination.registerAndAwait("clientsStart", numClients)


    val dataLoader = classOf[DataLoader].newInstance.parse(new String(clusterRoot.awaitChild("clusterReady").data))
    val numServers = dataLoader.numServers

    val cluster = new ScadsCluster(clusterRoot)
    val servers = cluster.getAvailableServers

    val nsMin = cluster.getNamespace[IntRec, LongRec]("tweetIdMin")
    val nsMax = cluster.getNamespace[IntRec, LongRec]("tweetIdMax")
    val minId = nsMin.getRange(None, None).map(_._2.f1).min
    val maxId = nsMax.getRange(None, None).map(_._2.f1).max


    val ns = cluster.getNamespace[HashLongRec, StringRec]("tweets")

    val partitionList = None :: (1 until numServers).map(
            x => Some(LongRec((maxId - minId) / numServers * x + minId))
            ).toList zip servers.map(List(_))
    var nsOutput = cluster.createNamespace[LongRec, LongSeqRec](
        "retweetIndex", partitionList)

    // Execute map/reduce
    var start_ms = System.currentTimeMillis()
    ns.executeMapReduce[LongRec, LongSeqRec](
        None, None, classOf[RetweetMapper],
        Some(classOf[RetweetCombiner]), classOf[RetweetReducer],
        "retweetIndex", true)
    var elapsed_time_ms = System.currentTimeMillis() - start_ms;

    println("*****************************")
    println("********Final Result*********")
    println("*****************************")
//    nsOutput.getRange(None, None).foreach(t => println(t))
    nsOutput.dumpDistribution
    println

    println("\ntotal elapsed time (sec): " + elapsed_time_ms / 1000.0)
    println




    // Create output namespace
    val nsOutput2 = cluster.createNamespace[LongRec, RetweetGraphRec](
        "retweetGraph", partitionList)

    start_ms = System.currentTimeMillis()
    val ns4 = cluster.getNamespace[LongRec, LongSeqRec]("retweetIndex")
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

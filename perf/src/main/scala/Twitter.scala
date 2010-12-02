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
    if (clientId == 0) {
      cluster.blockUntilReady(numServers)

      cluster.getNamespace[LongRec, StringRec]("tweets")
    }

    coordination.registerAndAwait("startWrite", numLoaders)
    val ns = cluster.getNamespace[LongRec, StringRec]("tweets")

    // LARGE files start from 2428
    val startFile = 2428
    val numFilesToLoad = 1
    val filenameBase = "/work/marmbrus/twitter/ec2/"

    (0 until numFilesToLoad).view.map(numLoaders*_ + startFile + clientId)
        .filter(_ < (startFile + numFilesToLoad)).foreach(num => {
      val filename = filenameBase + "raw." + num + ".txt.gz"
      logger.info("Loader: " + clientId + ", loading file: " + filename)
      TwitterLoader.loadFile(filename, ns)
    })

    coordination.registerAndAwait("endWrite", numLoaders)

    if (clientId == 0) {
      clusterRoot.createChild("clusterReady", data = this.toJson.getBytes)
    }
  }
}

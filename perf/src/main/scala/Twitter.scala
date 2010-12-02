package edu.berkeley.cs.scads.perf
package twitter

import deploylib._
import deploylib.mesos._
import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.config._
import edu.berkeley.cs.scads.storage._
import edu.berkeley.cs.avro.runtime._
import edu.berkeley.cs.avro.marker._

import org.apache.zookeeper.CreateMode

case class DataLoader(var numServers: Int, var numLoaders: Int) extends DataLoadingAvroClient with AvroRecord {
  def run(clusterRoot: ZooKeeperProxy#ZooKeeperNode): Unit = {
    val coordination = clusterRoot.getOrCreate("coordination/loaders")
    val cluster = new ExperimentalScadsCluster(clusterRoot)

    val clientId = coordination.registerAndAwait("clientsStart", numLoader)
    if (clientId == 0) {
      cluster.blockUntilReady(numServers)

      cluster.getNamespace[IntRec, StringRec]("tweets")
    }

    coordination.registerAndAwait("startWrite", numLoaders)
    val ns = cluster.getNamespace[IntRec, StringRec]("tweets")

    val data = (1 to 10000).view.map(i => (IntRec(i), StringRec("Tweet")))

    val startTime = System.currentTimeMillis
    logger.info("Starting bulk put")
    ns ++= data
    logger.info("Bulk put complete")

    coordination.registerAndAwait("endWrite", numLoaders)

    if (clientId == 0)
      clusterRoot.createChild("clusterReady", data = this.toJson.getBytes)
  }
}

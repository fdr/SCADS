package scaletest

import deploylib._
import deploylib.config._
import deploylib.runit._

import java.io.File

object ScadsDeployment extends ConfigurationActions {
	val storageEnginePort = 9090
	val zookeeperPort = 2181

	def deployStorageEngine(target: RunitManager, zooServer: RemoteMachine, bulkLoad: Boolean): RunitService = {
    val bulkLoadFlag = if(bulkLoad) " -b " else ""
		createJavaService(target, new File("target/scale-1.0-SNAPSHOT-jar-with-dependencies.jar"),
			"edu.berkeley.cs.scads.storage.JavaEngine",
      1024,
			"-p " + storageEnginePort + " -z " + zooServer.hostname + ":" + zookeeperPort + bulkLoadFlag)
	}

	def deployZooKeeperServer(target: RunitManager): RunitService = {
		val zooStorageDir = createDirectory(target, new File(target.rootDirectory, "zookeeperdata"))
		val zooService = createJavaService(target, new File("target/scale-1.0-SNAPSHOT-jar-with-dependencies.jar"),
			"org.apache.zookeeper.server.quorum.QuorumPeerMain",
      1024,
			"zoo.cnf")
		val zooConfigData = Array("tickTime=2000", "initLimit=10", "syncLimit=5", "clientPort=" + zookeeperPort, "dataDir=" + zooStorageDir).mkString("", "\n", "\n")
		val zooConfigFile = createFile(target, new File(zooService.serviceDir, "zoo.cnf"), zooConfigData, "644")

		return zooService
	}
}

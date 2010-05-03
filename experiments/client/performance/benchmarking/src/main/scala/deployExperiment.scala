import deploylib._
import deploylib.ec2._
import deploylib.ParallelConversions._

import deploylib.runit._
import deploylib.config._
import deploylib.xresults._
import org.apache.log4j.Logger
import org.json.JSONObject
import org.json.JSONArray

import edu.berkeley.cs.scads.thrift._
import edu.berkeley.cs.scads.model.{Environment, TrivialSession, TrivialExecutor, ZooKeptCluster}
import scala.collection.jcl.Conversions._

import org.apache.commons.cli.Options
import org.apache.commons.cli.GnuParser
import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.HelpFormatter

import scala.xml._
import java.io._

//import edu.berkeley.cs.scads.scaletest._
//import scaletest._

object ClusterDeployment extends ConfigurationActions {
	def main(args:Array[String]) {
		// Get experiment params
		println("Experiment:")
		val whichOp = System.getProperty("whichOp").toInt			// only one at a time => take advantage of ||ism using EC2
		println("whichOp=>" + whichOp + "<")
		val numThreads = System.getProperty("numThreads").toInt		// only generate ops from a single machine
		println("numThreads=>" + numThreads + "<")
		val warmupDuration = System.getProperty("warmupDuration").toInt
		println("warmupDuration(s)=>" + warmupDuration + "<")
		val runDuration = System.getProperty("runDuration").toInt
		println("runDuration(s)=>" + runDuration + "<")
		
		// # data items:  A
		println("# data items (A):")
		val minItemsA = System.getProperty("minItemsA").toInt
		println("minItemsA=>" + minItemsA + "<")
		val maxItemsA = System.getProperty("maxItemsA").toInt
		println("maxItemsA=>" + maxItemsA + "<")
		val itemsIncA = System.getProperty("itemsIncA").toInt
		println("itemsIncA=>" + itemsIncA + "<")

		// # data items:  B
		println("# data items (B):")
		val minItemsB = System.getProperty("minItemsB").toInt
		println("minItemsB=>" + minItemsB + "<")
		val maxItemsB = System.getProperty("maxItemsB").toInt
		println("maxItemsB=>" + maxItemsB + "<")
		val itemsIncB = System.getProperty("itemsIncB").toInt
		println("itemsIncB=>" + itemsIncB + "<")

		// data size:  A
		println("Data size (A):")
		val minCharsA = System.getProperty("minCharsA").toInt
		println("minCharsA=>" + minCharsA + "<")
		val maxCharsA = System.getProperty("maxCharsA").toInt
		println("maxCharsA=>" + maxCharsA + "<")
		val charsIncA = System.getProperty("charsIncA").toInt
		println("charsIncA=>" + charsIncA + "<")
		
		// data size:  B
		println("Data size (B):")
		val minCharsB = System.getProperty("minCharsB").toInt
		println("minCharsB=>" + minCharsB + "<")
		val maxCharsB = System.getProperty("maxCharsB").toInt
		println("maxCharsB=>" + maxCharsB + "<")
		val charsIncB = System.getProperty("charsIncB").toInt
		println("charsIncB=>" + charsIncB + "<")

		// setup
		println("Setup:")
		val zookeeperPort = System.getProperty("zookeeperPort").toInt
		println("  zookeeperPort=>" + zookeeperPort + "<")
		val storageEnginePort = System.getProperty("storageEnginePort").toInt
		println("  storageEnginePort=>" + storageEnginePort + "<")
		val reservationID = System.getProperty("reservationID")
		println("  reservationID=>" + reservationID + "<")
		
		
		// Deploy cluster
		//val reservationID = EC2Instance.runInstancesAndReturnReservationID("ami-e7a2448e", 3, 3, System.getenv("AWS_KEY_NAME"), "m1.small", "us-east-1c")
		val nodes = EC2Instance.runInstances("ami-e7a2448e", 3, 3, System.getenv("AWS_KEY_NAME"), "m1.small", "us-east-1c")
		//val nodes = EC2Instance.myInstances
		//val nodes = EC2Instance.getReservation(reservationID)
		println(nodes)
		println(EC2Instance.myInstances)

		// Setup Zookeeper
		//val zookeeperPort = System.getProperty("zookeeperPort").toInt
		//println("zookeeperPort: " + zookeeperPort)
		
		val zooNode = nodes(0)
		val zooService = deployZooKeeperServer(zooNode, zookeeperPort)
		zooService.watchFailures
		zooService.start
		zooService.blockTillUpFor(5)
		zooNode.blockTillPortOpen(zookeeperPort)


		// Setup StorageNode
		val storageNode = nodes(1)
		
		//val storageEnginePort = System.getProperty("storageEnginePort").toInt
		//println("storageEnginePort: " + storageEnginePort)
		
		val storageService = deployStorageEngine(storageNode, zooNode, /*bulkLoad*/ false, storageEnginePort, zookeeperPort)
		storageService.watchFailures
		storageService.start
		storageService.blockTillUpFor(5)
		storageNode.blockTillPortOpen(storageEnginePort)
		
		
		// Setup OpBenchmarker
		//def createJavaService(target: RunitManager, localJar: File, className: String, maxHeapMb: Int, args: String): RunitService = {
		val clientNode = nodes(2)
		clientNode.executeCommand("rm -rf /mnt/services")
		clientNode.executeCommand("rm /root/*")
		/*
		val clientService = createJavaService(clientNode, 
			new File("/Users/ksauer/Desktop/scads/experiments/client/performance/benchmarking/target/benchmarker-1.0-SNAPSHOT-jar-with-dependencies.jar"),
			"BenchmarkOps",
			2048,
			"-whichOp=1 -numThreads=5 -warmupDuration=10 -runDuration=10 -minItems=5 -maxItems=10 " +  
			"-itemsInc=5 -minChars=5 -maxChars=10 -charsInc=5 -zookeeperServerAndPort=" + zooNode.hostname + ":" + zookeeperPort + 
			" -storageNodeServer=" + storageNode.hostname)
		*/
		
		/*
		val logFileText = Array("log4j.logger.scads.queryexecution.operators=INFO,A1",
			"log4j.appender.A1=org.apache.log4j.RollingFileAppender",
			"log4j.appender.A1.File=/root/benchmark-output.log",
			"log4j.appender.A1.layout=org.apache.log4j.PatternLayout",
			"log4j.appender.A1.layout.ConversionPattern=%d{DATE} [%p] %t: %m%n").mkString("", "\n", "\n")
		*/
		val logFileText = Array("log4j.logger.scads.queryexecution.operators=INFO,A1",
			"log4j.appender.A1=org.apache.log4j.RollingFileAppender",
			"log4j.appender.A1.File=/root/operator/benchmark-output.log",
			"log4j.appender.A1.layout=org.apache.log4j.PatternLayout",
			"log4j.appender.A1.layout.ConversionPattern=%d{DATE} [%p] %t: %m%n",
			"log4j.logger.scads.readpolicy.primitives=INFO,A2",
			"log4j.appender.A2=org.apache.log4j.RollingFileAppender",
			"log4j.appender.A2.File=/root/primitive/benchmark-output.log",
			"log4j.appender.A2.layout=org.apache.log4j.PatternLayout",
			"log4j.appender.A2.layout.ConversionPattern=%d{DATE} [%p] %t: %m%n").mkString("", "\n", "\n")
																						
		val clientService = createJavaServiceWithCustomLog4jConfigFile(clientNode, 
			new File("/Users/ksauer/Desktop/scads/experiments/client/performance/benchmarking/target/benchmarker-1.0-SNAPSHOT-jar-with-dependencies.jar"),
			//new File("/work/ksauer/benchmarker-1.0-SNAPSHOT-jar-with-dependencies.jar"),
			"BenchmarkOps",
			2048,
			/*
			"-whichOp=1 -numThreads=5 -warmupDuration=10 -runDuration=10 -minItems=5 -maxItems=10 " +  
			"-itemsInc=5 -minChars=5 -maxChars=10 -charsInc=5 -zookeeperServerAndPort=" + zooNode.hostname + ":" + zookeeperPort + 
			" -storageNodeServer=" + storageNode.hostname, logFileText)
			*/
			"-whichOp=" + whichOp + " -numThreads=" + numThreads + " -warmupDuration=" + warmupDuration + " -runDuration=" + runDuration
			+ " -minItemsA=" + minItemsA + " -maxItemsA=" + maxItemsA + " -itemsIncA=" + itemsIncA
			+ " -minItemsB=" + minItemsB + " -maxItemsB=" + maxItemsB + " -itemsIncB=" + itemsIncB
			+ " -minCharsA=" + minCharsA + " -maxCharsA=" + maxCharsA + " -charsIncA=" + charsIncA
			+ " -minCharsB=" + minCharsB + " -maxCharsB=" + maxCharsB + " -charsIncB=" + charsIncB
			+ " -zookeeperServerAndPort=" + zooNode.hostname + ":" + zookeeperPort
			+ " -storageNodeServer=" + storageNode.hostname,
			logFileText)
		clientService.watchFailures
		//clientService.start
		clientService.once
		clientService.blockTillDown
		clientNode.executeCommand("tar -cvvf op" + whichOp + ".tar /root/*")
		//clientNode.download(new File("/root/op" + whichOp + ".tar"), new File("/work/ksauer/4.30.10-op-benchmarking-logs"))
		clientNode.download(new File("/root/op" + whichOp + ".tar"), new File("/Users/ksauer/Desktop"))
		
		
		
		/*
		val jarFilename = System.getProperty("jarFilename")
		println("jar filename: " + jarFilename)
		val jarLocalDir = System.getProperty("jarLocalDir")
		println("jar local dir: " + jarLocalDir)
		val remoteDir = System.getProperty("remoteDir")
		println("remote dir: " + remoteDir)
		
		clientNode.upload(new File(jarLocalDir + "/" + jarFilename), new File(remoteDir))
		clientNode.executeCommand("java -cp " + remoteDir + "/" + jarFilename + " BenchmarkOps -whichOp=1 -numThreads=5 -warmupDuration=10" + 
			" -runDuration=10 -minItems=5 -maxItems=10 " +  
			"-itemsInc=5 -minChars=5 -maxChars=10 -charsInc=5 -zookeeperServerAndPort=" + zooNode.hostname + ":" + zookeeperPort + 
			" -storageNodeServer=" + storageNode.hostname + " > out.txt")
		*/
	}
	
	
	def deployZooKeeperServer(target: RunitManager, zookeeperPort:Int): RunitService = {
		val zooStorageDir = createDirectory(target, new File(target.rootDirectory, "zookeeperdata"))
		val zooService = createJavaService(target, 
			new File("/Users/ksauer/Desktop/scads/scalaengine/target/scalaengine-1.1-SNAPSHOT-jar-with-dependencies.jar"),
			//new File("/work/ksauer/scalaengine-1.1-SNAPSHOT-jar-with-dependencies.jar"),
			"org.apache.zookeeper.server.quorum.QuorumPeerMain",
      		2048,
			"zoo.cnf")
		val config = <configuration type="zookeeper">
					<tickTime>20000</tickTime>
					<initLimit>10</initLimit>
					<syncLimit>5</syncLimit>
					<clientPort>{zookeeperPort}</clientPort>
					<dataDir>{zooStorageDir}</dataDir>
					</configuration>

		val zooConfigData = config.descendant.filter(_.getClass == classOf[scala.xml.Elem]).map(e => e.label + "=" + e.text).mkString("", "\n", "\n")
		val zooConfigFile = createFile(target, new File(zooService.serviceDir, "zoo.cnf"), zooConfigData, "644")

		//XResult.storeXml(config)

		return zooService
	}
	
	
	def deployStorageEngine(target: RunitManager, zooServer: RemoteMachine, bulkLoad: Boolean, storageEnginePort:Int, zookeeperPort:Int): RunitService = {
    val bulkLoadFlag = if(bulkLoad) " -b " else ""
		createJavaService(target, 
			new File("/Users/ksauer/Desktop/scads/scalaengine/target/scalaengine-1.1-SNAPSHOT-jar-with-dependencies.jar"),
			//new File("/work/ksauer/scalaengine-1.1-SNAPSHOT-jar-with-dependencies.jar"),
			"edu.berkeley.cs.scads.storage.JavaEngine",
      		2048,
			"-p " + storageEnginePort + " -z " + zooServer.hostname + ":" + zookeeperPort + bulkLoadFlag)
	}
	
}

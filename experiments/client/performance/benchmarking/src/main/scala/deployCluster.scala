import deploylib._
import deploylib.ec2._
import deploylib.ParallelConversions._

import deploylib.runit._
import deploylib.config._
import deploylib.xresults._
import org.apache.log4j.Logger
import org.json.JSONObject
import org.json.JSONArray

import com.amazonaws.ec2._
import com.amazonaws.ec2.model._

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
		val availabilityZone = "us-east-1b"
		val zookeeperPort = 2181
		val storageEnginePort = 9000
		
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
		val oneBin = System.getProperty("oneBin").toBoolean
		println("oneBin=>" + oneBin + "<")
		
		// # data items:  A
		println("# data items (A):")
		val minItemsA = System.getProperty("minItemsA").toInt
		println("minItemsA=>" + minItemsA + "<")
		
		var maxItemsA = minItemsA	// same if oneBin
		var itemsIncA = 0
		
		if (!oneBin) {
			maxItemsA = System.getProperty("maxItemsA").toInt
			println("maxItemsA=>" + maxItemsA + "<")
			itemsIncA = System.getProperty("itemsIncA").toInt
			println("itemsIncA=>" + itemsIncA + "<")
		}

		// # data items:  B
		println("# data items (B):")
		val minItemsB = System.getProperty("minItemsB").toInt
		println("minItemsB=>" + minItemsB + "<")
		
		var maxItemsB = minItemsB	// same if oneBin
		var itemsIncB = 0
		
		if (!oneBin) {
			maxItemsB = System.getProperty("maxItemsB").toInt
			println("maxItemsB=>" + maxItemsB + "<")
			itemsIncB = System.getProperty("itemsIncB").toInt
			println("itemsIncB=>" + itemsIncB + "<")
		}

		// data size:  A
		println("Data size (A):")
		val minCharsA = System.getProperty("minCharsA").toInt
		println("minCharsA=>" + minCharsA + "<")
		
		var maxCharsA = minCharsA	// same if oneBin
		var charsIncA = 0
		
		if (!oneBin) {
			maxCharsA = System.getProperty("maxCharsA").toInt
			println("maxCharsA=>" + maxCharsA + "<")
			charsIncA = System.getProperty("charsIncA").toInt
			println("charsIncA=>" + charsIncA + "<")
		}
		
		// data size:  B
		println("Data size (B):")
		val minCharsB = System.getProperty("minCharsB").toInt
		println("minCharsB=>" + minCharsB + "<")
		
		var maxCharsB = minCharsB	// same if oneBin
		var charsIncB = 0
		
		if (!oneBin) {
			maxCharsB = System.getProperty("maxCharsB").toInt
			println("maxCharsB=>" + maxCharsB + "<")
			charsIncB = System.getProperty("charsIncB").toInt
			println("charsIncB=>" + charsIncB + "<")
		}

		// setup
		println("Setup:")
		val remoteDir = System.getProperty("remoteDir")
		println("  remoteDir=>" + remoteDir + "<")
		val bucket = System.getProperty("bucket")
		println("  bucket=>" + bucket + "<")
		val filenamePrefix = System.getProperty("filenamePrefix")
		println("  filenamePrefix=>" + filenamePrefix + "<")
		
		
		// Deploy cluster
		println(System.getenv("AWS_KEY_NAME"))
		println(availabilityZone)
		val nodes = EC2Instance.runInstances("ami-e7a2448e", 3, 3, System.getenv("AWS_KEY_NAME"), "m1.small", availabilityZone)
		println(nodes)

		// Setup Zookeeper
		val zooNode = nodes(0)
		val zooService = deployZooKeeperServer(zooNode, zookeeperPort)
		zooService.watchFailures
		zooService.start
		zooService.blockTillUpFor(5)
		zooNode.blockTillPortOpen(zookeeperPort)

		// Setup StorageNode
		val storageNode = nodes(1)
		val storageService = deployStorageEngine(storageNode, zooNode, /*bulkLoad*/ false, storageEnginePort, zookeeperPort)
		storageService.watchFailures
		storageService.start
		storageService.blockTillUpFor(5)
		storageNode.blockTillPortOpen(storageEnginePort)
		
		
		// Setup OpBenchmarker
		val clientNode = nodes(2)
		clientNode.executeCommand("mkdir /root/operator")
		clientNode.executeCommand("mkdir /root/primitive")
		/*		
		val logFileText = Array("log4j.logger.scads.queryexecution.operators=INFO,A1",
			"log4j.appender.A1=org.apache.log4j.RollingFileAppender",
			"log4j.appender.A1.File=/root/benchmark-output.log",
			"log4j.appender.A1.layout=org.apache.log4j.PatternLayout",
			"log4j.appender.A1.layout.ConversionPattern=%d{DATE} [%p] %t: %m%n").mkString("", "\n", "\n")
		*/
		
		val logFileText = Array("log4j.logger.scads.queryexecution.operators=INFO,A1",
			//"log4j.appender.A1=org.apache.log4j.RollingFileAppender",
			"log4j.appender.A1=org.apache.log4j.FileAppender",
			"log4j.appender.A1.File=/root/operator/benchmark-output.log",
			"log4j.appender.A1.layout=org.apache.log4j.PatternLayout",
			"log4j.appender.A1.layout.ConversionPattern=%d{DATE} [%p] %t: %m%n",
			"log4j.logger.scads.readpolicy.primitives=INFO,A2",
			//"log4j.appender.A2=org.apache.log4j.RollingFileAppender",
			"log4j.appender.A2=org.apache.log4j.FileAppender",
			"log4j.appender.A2.File=/root/primitive/benchmark-output.log",
			"log4j.appender.A2.layout=org.apache.log4j.PatternLayout",
			"log4j.appender.A2.layout.ConversionPattern=%d{DATE} [%p] %t: %m%n").mkString("", "\n", "\n")

		val clientArgs = ("-whichOp=" + whichOp + " -numThreads=" + numThreads + " -warmupDuration=" + warmupDuration 
			+ " -runDuration=" + runDuration + " -oneBin=" + oneBin
			+ " -minItemsA=" + minItemsA + " -maxItemsA=" + maxItemsA + " -itemsIncA=" + itemsIncA
			+ " -minItemsB=" + minItemsB + " -maxItemsB=" + maxItemsB + " -itemsIncB=" + itemsIncB
			+ " -minCharsA=" + minCharsA + " -maxCharsA=" + maxCharsA + " -charsIncA=" + charsIncA
			+ " -minCharsB=" + minCharsB + " -maxCharsB=" + maxCharsB + " -charsIncB=" + charsIncB
			+ " -zookeeperServerAndPort=" + zooNode.hostname + ":" + zookeeperPort
			+ " -storageNodeServer=" + storageNode.hostname)
																						
		val clientService = createJavaServiceWithCustomLog4jConfigFile(clientNode, 
			//new File("/Users/ksauer/Desktop/scads/experiments/client/performance/benchmarking/target/benchmarker-1.0-SNAPSHOT-jar-with-dependencies.jar"),
			new File(remoteDir + "/benchmarker-1.0-SNAPSHOT-jar-with-dependencies.jar"),
			//new File("/work/ksauer/benchmarker-1.0-SNAPSHOT-jar-with-dependencies.jar"),
			"BenchmarkOps",
			//"BenchmarkOpsNoPrimLogging",
			2048,
			/*
			"-whichOp=1 -numThreads=5 -warmupDuration=10 -runDuration=10 -minItems=5 -maxItems=10 " +  
			"-itemsInc=5 -minChars=5 -maxChars=10 -charsInc=5 -zookeeperServerAndPort=" + zooNode.hostname + ":" + zookeeperPort + 
			" -storageNodeServer=" + storageNode.hostname, logFileText)
			*/
			clientArgs,
			logFileText)
		clientService.watchFailures
		//clientService.start
		clientService.once
		clientService.blockTillDown
		//clientNode.executeCommand("tar -cvvf op" + whichOp + ".tar /root/*")
		//clientNode.executeCommand("s3cmd put /root/op" + whichOp + ".tar s3://kristal")
		clientNode.executeCommand("cd " + clientNode.rootDirectory)
		clientNode.executeCommand("s3cmd get s3://kristal/logparsing-1.0-SNAPSHOT-jar-with-dependencies.jar")
		clientNode.executeCommand("java -Xmx1G -DlogDir=operator -DoutputFilename=op"+whichOp+"ops.csv -cp logparsing-1.0-SNAPSHOT-jar-with-dependencies.jar parser.ParseOperatorLogs")
		//clientNode.executeCommand("java -Xmx1G -DlogDir=primitive -DoutputFilename=op"+whichOp+"prims.csv -cp logparsing-1.0-SNAPSHOT-jar-with-dependencies.jar parser.ParseOperatorLogs")
		// need to bin the prims -- use piql-binner
		clientNode.executeCommand("s3cmd get s3://kristal/piql-binner.jar")
		clientNode.executeCommand("java -Xmx1G -cp piql-binner.jar parsing.PrimitiveBinner primitive/ primitive/bins")
		
		clientNode.executeCommand("rm *.jar")
		//val filename = "op" + whichOp + "-" + numThreads + "-threads.tar"
		/*
		clientNode.executeCommand("tar -cvvf " + filename + " *")
		clientNode.executeCommand("gzip " + filename)
		clientNode.executeCommand("s3cmd put " + filename + ".gz s3://kristal/run-" + bucketSuffix + "/" + filename + ".gz")
		*/
		clientNode.executeCommand("tar -cvvf " + filenamePrefix + ".tar *")
		clientNode.executeCommand("gzip " + filenamePrefix + ".tar")
		clientNode.executeCommand("s3cmd put " + filenamePrefix + ".tar.gz s3://" + bucket + "/" + filenamePrefix + ".tar.gz")
		
		println("zookeeper: " + zooNode.hostname)
		println("storage engine: " + storageNode.hostname)
		println("client: " + clientNode.hostname)
	}
	
	
	def deployZooKeeperServer(target: RunitManager, zookeeperPort:Int): RunitService = {
		val remoteDir = System.getProperty("remoteDir")
		
		val zooStorageDir = createDirectory(target, new File(target.rootDirectory, "zookeeperdata"))
		val zooService = createJavaService(target, 
			//new File("/Users/ksauer/Desktop/scads/scalaengine/target/scalaengine-1.1-SNAPSHOT-jar-with-dependencies.jar"),
			new File(remoteDir + "/scalaengine-1.1-SNAPSHOT-jar-with-dependencies.jar"),
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
		val remoteDir = System.getProperty("remoteDir")
    	val bulkLoadFlag = if(bulkLoad) " -b " else ""
		createJavaService(target, 
			//new File("/Users/ksauer/Desktop/scads/scalaengine/target/scalaengine-1.1-SNAPSHOT-jar-with-dependencies.jar"),
			new File(remoteDir + "/scalaengine-1.1-SNAPSHOT-jar-with-dependencies.jar"),
			//new File("/work/ksauer/scalaengine-1.1-SNAPSHOT-jar-with-dependencies.jar"),
			"edu.berkeley.cs.scads.storage.JavaEngine",
      		2048,
			"-p " + storageEnginePort + " -z " + zooServer.hostname + ":" + zookeeperPort + bulkLoadFlag)
	}
	
}

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

//import edu.berkeley.cs.scads.thrift._
//import edu.berkeley.cs.scads.model.{Environment, TrivialSession, TrivialExecutor, ZooKeptCluster}
import edu.berkeley.cs.scads.piql._
import edu.berkeley.cs.scads.piql.parser._

//import scala.collection.jcl.Conversions._

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
		// These params don't change from run to run
		val availabilityZone = "us-east-1c"
		val zookeeperPort = 2181
		val storageEnginePort = 9000
		val remoteDir = "/root"
		
		/*
		System.setenv("AWS_KEY_NAME", "kristal")
		System.setenv("AWS_SECRET_ACCESS_KEY", "UXZ7FDk74XQk4NxgN9K0oK6iot7eL1A1V/6xX2LB")
		System.setenv("AWS_ACCESS_KEY_ID", "026FE3D736A8XTV3D382")
		System.setenv("AWS_KEY_PATH", "/root/kristal")
		*/
		
		// Cmd line args
		val options = new Options();
		// Experiment
		options.addOption("whichOp", true, "specify operator to benchmark")
		options.addOption("oneBin", true, "indicates that you only want one bin")
		options.addOption("numThreads", true, "how many threads")
		options.addOption("warmupDuration", true, "duration of jvm warmup (s)")
		options.addOption("runDuration", true, "duration of run (s)")
		
		// # data items:  A
		options.addOption("minItemsA", true, "min items of type A")
		options.addOption("maxItemsA", true, "max items of type A")
		options.addOption("itemsIncA", true, "items inc of type A")

		// # data items:  B
		options.addOption("minItemsB", true, "min items of type B")
		options.addOption("maxItemsB", true, "max items of type B")
		options.addOption("itemsIncB", true, "items inc of type B")

		// Data size:  A
		options.addOption("minCharsA", true, "min chars of type A")
		options.addOption("maxCharsA", true, "max chars of type A")
		options.addOption("charsIncA", true, "chars inc of type A")
		
		// Data size:  B
		options.addOption("minCharsB", true, "min chars of type B")
		options.addOption("maxCharsB", true, "max chars of type B")
		options.addOption("charsIncB", true, "chars inc of type B")
		
		// Setup
		options.addOption("bucket", true, "bucket on S3 for uploading run files")
		options.addOption("filenamePrefix", true, "prefix of run filename")
		

		val parser = new GnuParser();
		val cmd = parser.parse(options, args);

		// Get args from cmd line
		// Experiment
		val whichOp = cmd.getOptionValue("whichOp").toInt
		println("Benchmarking op " + whichOp + "...")
		val oneBin = cmd.getOptionValue("oneBin").toBoolean
		println("One bin? " + oneBin)
		val numThreads = cmd.getOptionValue("numThreads").toInt
		println("Using " + numThreads + " threads.")
		val warmupDuration = cmd.getOptionValue("warmupDuration").toInt
		println("Warmup duration (s):  " + warmupDuration)
		val runDuration = cmd.getOptionValue("runDuration").toInt
		println("Run duration (s):  " + runDuration)
		
		// # data items:  A
		val minItemsA = cmd.getOptionValue("minItemsA").toInt
		println("Min items (A): " + minItemsA)
		
		var maxItemsA = minItemsA
		var itemsIncA = 0
		
		if (!oneBin) {
			val maxItemsA = cmd.getOptionValue("maxItemsA").toInt
			println("Max items (A): " + maxItemsA)
			val itemsIncA = cmd.getOptionValue("itemsIncA").toInt
			println("Items inc (A): " + itemsIncA)
		}
		
		// # data items:  B
		val minItemsB = cmd.getOptionValue("minItemsB").toInt
		println("Min items (B): " + minItemsB)
		
		var maxItemsB = minItemsB
		var itemsIncB = 0
		
		if (!oneBin) {
			val maxItemsB = cmd.getOptionValue("maxItemsB").toInt
			println("Max items (B): " + maxItemsB)
			val itemsIncB = cmd.getOptionValue("itemsIncB").toInt
			println("Items inc (B): " + itemsIncB)
		}
				
		// Data size:  A
		val minCharsA = cmd.getOptionValue("minCharsA").toInt
		println("Min chars (A): " + minCharsA)
		
		var maxCharsA = minCharsA
		var charsIncA = 0
		
		if (!oneBin) {
			val maxCharsA = cmd.getOptionValue("maxCharsA").toInt
			println("Max chars (A): " + maxCharsA)
			val charsIncA = cmd.getOptionValue("charsIncA").toInt
			println("Chars inc (A): " + charsIncA)
		}

		// Data size:  B
		val minCharsB = cmd.getOptionValue("minCharsB").toInt
		println("Min chars (B): " + minCharsB)
		
		var maxCharsB = minCharsB
		var charsIncB = 0
		
		if (!oneBin) {
			val maxCharsB = cmd.getOptionValue("maxCharsB").toInt
			println("Max chars (B): " + maxCharsB)
			val charsIncB = cmd.getOptionValue("charsIncB").toInt
			println("Chars inc (B): " + charsIncB)
		}

		// Setup
		println("Setup:")
		val bucket = cmd.getOptionValue("bucket")
		println("  S3 bucket: " + bucket)
		val filenamePrefix = cmd.getOptionValue("filenamePrefix")
		println("  Filename prefix: " + filenamePrefix)
		
		
		// Deploy cluster
		println(System.getenv("USER"))
		println(System.getenv("AWS_KEY_NAME"))
		println(System.getenv())
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


		// UPDATE:  pass in args.mkString("", "\n", "\n") + zookeeper and storagenode params
		val clientArgs = ("-whichOp=" + whichOp + " -numThreads=" + numThreads + " -warmupDuration=" + warmupDuration 
			+ " -runDuration=" + runDuration + " -oneBin=" + oneBin
			+ " -minItemsA=" + minItemsA + " -maxItemsA=" + maxItemsA + " -itemsIncA=" + itemsIncA
			+ " -minItemsB=" + minItemsB + " -maxItemsB=" + maxItemsB + " -itemsIncB=" + itemsIncB
			+ " -minCharsA=" + minCharsA + " -maxCharsA=" + maxCharsA + " -charsIncA=" + charsIncA
			+ " -minCharsB=" + minCharsB + " -maxCharsB=" + maxCharsB + " -charsIncB=" + charsIncB
			+ " -zookeeperServerAndPort=" + zooNode.privateDnsName + ":" + zookeeperPort
			+ " -storageNodeServer=" + storageNode.privateDnsName)
																						
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
		clientNode.executeCommand("java -Xmx1G -DlogDir=primitive -DoutputFilename=op"+whichOp+"prims.csv -cp logparsing-1.0-SNAPSHOT-jar-with-dependencies.jar parser.ParseOperatorLogs")
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
		
		println("zookeeper: " + zooNode.privateDnsName)
		println("storage engine: " + storageNode.privateDnsName)
		println("client: " + clientNode.privateDnsName)
	}
	
	
	def deployZooKeeperServer(target: RunitManager, zookeeperPort:Int): RunitService = {
		//val remoteDir = System.getProperty("remoteDir")
		val remoteDir = "/root"
		
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
	
	
	//def deployStorageEngine(target: RunitManager, zooServer: RemoteMachine, bulkLoad: Boolean, storageEnginePort:Int, zookeeperPort:Int): RunitService = {
	def deployStorageEngine(target: RunitManager, zooServer: EC2Instance, bulkLoad: Boolean, storageEnginePort:Int, zookeeperPort:Int): RunitService = {
		//val remoteDir = System.getProperty("remoteDir")
		val remoteDir = "/root"
		
    	val bulkLoadFlag = if(bulkLoad) " -b " else ""
		createJavaService(target, 
			//new File("/Users/ksauer/Desktop/scads/scalaengine/target/scalaengine-1.1-SNAPSHOT-jar-with-dependencies.jar"),
			new File(remoteDir + "/scalaengine-1.1-SNAPSHOT-jar-with-dependencies.jar"),
			//new File("/work/ksauer/scalaengine-1.1-SNAPSHOT-jar-with-dependencies.jar"),
			"edu.berkeley.cs.scads.storage.JavaEngine",
      		2048,
			"-p " + storageEnginePort + " -z " + zooServer.privateDnsName + ":" + zookeeperPort + bulkLoadFlag)
	}
	
}

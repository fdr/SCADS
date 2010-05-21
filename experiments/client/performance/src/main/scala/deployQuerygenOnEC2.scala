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

import org.apache.commons.cli._

import scala.xml._
import java.io._


object DeployQuerygen extends ConfigurationActions {
	def main(args:Array[String]) {
		//val scalaengineJarDir = "/Users/radlab/Desktop/ksauer/Desktop/scads/scalaengine/target"
		//val querygenJarDir = "/Users/radlab/Desktop/ksauer/Desktop/scads/experiments/client/performance/target"
		val scalaengineJarDir = "/root"
		val querygenJarDir = "/root"

		val options = new Options();
		val opLogger = Logger.getLogger("scads.queryexecution.operators")
		val primitiveLogger = Logger.getLogger("scads.readpolicy.primitives")
		val queryLogger = Logger.getLogger("queryLogger")
		

		// Setup cmd line args
		options.addOption("availabilityZone", true, "availability zone for EC2 instances")
		options.addOption("zookeeperPort", true, "zookeeper port")
		options.addOption("storageEnginePort", true, "storage engine port")
		
		// Data params
		options.addOption("numUsers", true, "how many users")
		options.addOption("thoughtsPerUser", true, "thoughts per user")
		options.addOption("subsPerUser", true, "subs per user")
		options.addOption("emailDupFactor", true, "# users per email")
		options.addOption("thoughtstreamLength", true, "# thoughts in each user's thoughtstream")
		options.addOption("numHashTagsPerThought", true, "how many hash tags get associated with each thought")
		options.addOption("numDistinctHashTags", true, "# distinct hash tags")

		// Experiment params
		options.addOption("numThreads", true, "num threads")
		options.addOption("mixChoice", true, "which mix matrix to use")
		options.addOption("duration", true, "duration of run (s)")
		options.addOption("warmupDuration", true, "duration of warmup (s)")
		
		// Get cmd line args
		val parser = new GnuParser();
		val cmd = parser.parse(options, args);
		
		val availabilityZone = cmd.getOptionValue("availabilityZone")
		opLogger.info("availabilityZone: " + availabilityZone)
		primitiveLogger.info("availabilityZone: " + availabilityZone)
		queryLogger.info("availabilityZone: " + availabilityZone)
		
		val zookeeperPort = cmd.getOptionValue("zookeeperPort").toInt
		opLogger.info("zookeeperPort: " + zookeeperPort)
		primitiveLogger.info("zookeeperPort: " + zookeeperPort)
		queryLogger.info("zookeeperPort: " + zookeeperPort)
		
		val storageEnginePort = cmd.getOptionValue("storageEnginePort").toInt
		opLogger.info("storageEnginePort: " + storageEnginePort)
		primitiveLogger.info("storageEnginePort: " + storageEnginePort)
		queryLogger.info("storageEnginePort: " + storageEnginePort)
		
		opLogger.info("Data parameters:")
		primitiveLogger.info("Data parameters:")
		queryLogger.info("Data parameters:")
		val numUsers = cmd.getOptionValue("numUsers").toInt
		opLogger.info("numUsers: " + numUsers)
		primitiveLogger.info("numUsers: " + numUsers)
		queryLogger.info("numUsers: " + numUsers)
		
		val thoughtsPerUser = cmd.getOptionValue("thoughtsPerUser").toInt
		opLogger.info("thoughtsPerUser: " + thoughtsPerUser)
		primitiveLogger.info("thoughtsPerUser: " + thoughtsPerUser)
		queryLogger.info("thoughtsPerUser: " + thoughtsPerUser)
		
		val subsPerUser = cmd.getOptionValue("subsPerUser").toInt
		opLogger.info("subsPerUser: " + subsPerUser)
		primitiveLogger.info("subsPerUser: " + subsPerUser)
		queryLogger.info("subsPerUser: " + subsPerUser)
		
		val emailDupFactor = cmd.getOptionValue("emailDupFactor").toInt
		opLogger.info("emailDupFactor: " + emailDupFactor)
		primitiveLogger.info("emailDupFactor: " + emailDupFactor)
		queryLogger.info("emailDupFactor: " + emailDupFactor)
		
		val thoughtstreamLength = cmd.getOptionValue("thoughtstreamLength").toInt
		opLogger.info("thoughtstreamLength: " + thoughtstreamLength)
		primitiveLogger.info("thoughtstreamLength: " + thoughtstreamLength)
		queryLogger.info("thoughtstreamLength: " + thoughtstreamLength)
		
		val numHashTagsPerThought = cmd.getOptionValue("numHashTagsPerThought").toInt
		opLogger.info("numHashTagsPerThought: " + numHashTagsPerThought)
		primitiveLogger.info("numHashTagsPerThought: " + numHashTagsPerThought)
		queryLogger.info("numHashTagsPerThought: " + numHashTagsPerThought)
		
		val numDistinctHashTags = cmd.getOptionValue("numDistinctHashTags").toInt
		opLogger.info("numDistinctHashTags: " + numDistinctHashTags)
		primitiveLogger.info("numDistinctHashTags: " + numDistinctHashTags)
		queryLogger.info("numDistinctHashTags: " + numDistinctHashTags)
		
		opLogger.info("Experiment parameters:")
		primitiveLogger.info("Experiment parameters:")
		queryLogger.info("Experiment parameters:")
		val numThreads = cmd.getOptionValue("numThreads").toInt
		opLogger.info("numThreads: " + numThreads)
		primitiveLogger.info("numThreads: " + numThreads)
		queryLogger.info("numThreads: " + numThreads)
		
		val mixChoice = cmd.getOptionValue("mixChoice")
		opLogger.info("mixChoice: " + mixChoice)
		primitiveLogger.info("mixChoice: " + mixChoice)
		queryLogger.info("mixChoice: " + mixChoice)
		
		val duration = cmd.getOptionValue("duration").toInt
		opLogger.info("duration: " + duration)
		primitiveLogger.info("duration: " + duration)
		queryLogger.info("duration: " + duration)
		
		val warmupDuration = cmd.getOptionValue("warmupDuration").toInt
		opLogger.info("warmupDuration: " + warmupDuration)
		primitiveLogger.info("warmupDuration: " + warmupDuration)
		queryLogger.info("warmupDuration: " + warmupDuration)
		
		
		
		
		// Start experiment
		val nodes = EC2Instance.runInstances("ami-e7a2448e", 3, 3, System.getenv("AWS_KEY_NAME"), "m1.small", availabilityZone)
		
		// Setup Zookeeper
		val zooNode = nodes(0)
		val zooService = deployZooKeeperServer(scalaengineJarDir, zooNode, zookeeperPort)
		zooService.watchFailures
		zooService.start
		zooService.blockTillUpFor(5)
		zooNode.blockTillPortOpen(zookeeperPort)


		// Setup StorageNode
		val storageNode = nodes(1)
		val storageService = deployStorageEngine(scalaengineJarDir, storageNode, zooNode, /*bulkLoad*/ false, storageEnginePort, zookeeperPort)
		storageService.watchFailures
		storageService.start
		storageService.blockTillUpFor(5)
		storageNode.blockTillPortOpen(storageEnginePort)
		
		
		// Setup querygen
		val clientNode = nodes(2)
		clientNode.executeCommand("mkdir /root/operator")
		clientNode.executeCommand("mkdir /root/primitive")
		clientNode.executeCommand("mkdir /root/query")
				
		val logFileText = Array("log4j.logger.scads.queryexecution.operators=INFO,A1",
			"log4j.appender.A1=org.apache.log4j.FileAppender",
			"log4j.appender.A1.File=/root/operator/output.log",
			"log4j.appender.A1.layout=org.apache.log4j.PatternLayout",
			"log4j.appender.A1.layout.ConversionPattern=%d{DATE} [%p] %t: %m%n",
			"log4j.logger.scads.readpolicy.primitives=INFO,A2",
			"log4j.appender.A2=org.apache.log4j.FileAppender",
			"log4j.appender.A2.File=/root/primitive/output.log",
			"log4j.appender.A2.layout=org.apache.log4j.PatternLayout",
			"log4j.appender.A2.layout.ConversionPattern=%d{DATE} [%p] %t: %m%n",
			"log4j.logger.queryLogger=INFO,A3",
			"log4j.appender.A3=org.apache.log4j.FileAppender",
			"log4j.appender.A3.File=/root/query/output.log",
			"log4j.appender.A3.layout=org.apache.log4j.PatternLayout",
			"log4j.appender.A3.layout.ConversionPattern=%d{DATE} [%p] %t: %m%n").mkString("", "\n", "\n")
																						
		val clientService = createJavaServiceWithCustomLog4jConfigFile(clientNode, 
			new File(querygenJarDir + "/querygen-1.0-SNAPSHOT-jar-with-dependencies.jar"),
			"WorkloadRunnerForEC2",
			2048,
			"-numUsers=" + numUsers + " -thoughtsPerUser=" + thoughtsPerUser + " -subsPerUser=" + subsPerUser + " -emailDupFactor=" + emailDupFactor +
			" -thoughtstreamLength=" + thoughtstreamLength + " -numHashTagsPerThought=" + numHashTagsPerThought + " -numDistinctHashTags=" + numDistinctHashTags + 
			" -numThreads=" + numThreads + " -mixChoice=" + mixChoice + " -storageNodeServer=" + storageNode.hostname + 
			" -zookeeperServerAndPort=" + zooNode.hostname + ":" + zookeeperPort + 
			" -duration=" + duration + " -warmupDuration=" + warmupDuration,
			logFileText)
		clientService.watchFailures
		clientService.once
		clientService.blockTillDown
		//clientNode.executeCommand("tar -cvvf op" + whichOp + ".tar /root/*")
		//clientNode.executeCommand("s3cmd put /root/op" + whichOp + ".tar s3://kristal")
		
		// put files up to s3
		clientNode.executeCommand("cd /root")
		clientNode.executeCommand("s3cmd get s3://kristal/logparsing-1.0-SNAPSHOT-jar-with-dependencies.jar")
		clientNode.executeCommand("java -DlogDir=query -DoutputFilename=queries.csv -cp logparsing-1.0-SNAPSHOT-jar-with-dependencies.jar parser.ParseOperatorLogs")
		clientNode.executeCommand("java -DlogDir=operator -DoutputFilename=ops.csv -cp logparsing-1.0-SNAPSHOT-jar-with-dependencies.jar parser.ParseOperatorLogs")
		clientNode.executeCommand("java -DlogDir=primitive -DoutputFilename=prims.csv -cp logparsing-1.0-SNAPSHOT-jar-with-dependencies.jar parser.ParseOperatorLogs")
		
		clientNode.executeCommand("s3cmd get s3://kristal/piql-binner.jar")
		clientNode.executeCommand("java -Xmx1G -cp piql-binner.jar parsing.PrimitiveBinner primitive/ primitive/bins")
		
		clientNode.executeCommand("rm *.jar")
		clientNode.executeCommand("tar -cvvf " + mixChoice + ".tar *")
		clientNode.executeCommand("gzip " + mixChoice + ".tar")
		clientNode.executeCommand("s3cmd put " + mixChoice + ".tar.gz s3://kristal")
		
	}
	
	
	def deployZooKeeperServer(jarDir:String, target: RunitManager, zookeeperPort:Int): RunitService = {
		val zooStorageDir = createDirectory(target, new File(target.rootDirectory, "zookeeperdata"))
		val zooService = createJavaService(target, 
			new File(jarDir + "/scalaengine-1.1-SNAPSHOT-jar-with-dependencies.jar"),
			"org.apache.zookeeper.server.quorum.QuorumPeerMain",
      		2048,
			"/mnt/services/org.apache.zookeeper.server.quorum.QuorumPeerMain/zoo.cnf")
		val config = <configuration type="zookeeper">
					<tickTime>20000</tickTime>
					<initLimit>10</initLimit>
					<syncLimit>5</syncLimit>
					<clientPort>{zookeeperPort}</clientPort>
					<dataDir>{zooStorageDir}</dataDir>
					</configuration>

		val zooConfigData = config.descendant.filter(_.getClass == classOf[scala.xml.Elem]).map(e => e.label + "=" + e.text).mkString("", "\n", "\n")
		val zooConfigFile = createFile(target, new File(zooService.serviceDir, "zoo.cnf"), zooConfigData, "644")

		return zooService
	}
	
	
	def deployStorageEngine(jarDir:String, target: RunitManager, zooServer: RemoteMachine, bulkLoad: Boolean, storageEnginePort:Int, zookeeperPort:Int): RunitService = {
    	val bulkLoadFlag = if(bulkLoad) " -b " else ""
		createJavaService(target, 
			new File(jarDir + "/scalaengine-1.1-SNAPSHOT-jar-with-dependencies.jar"),
			"edu.berkeley.cs.scads.storage.JavaEngine",
      		2048,
			"-p " + storageEnginePort + " -z " + zooServer.hostname + ":" + zookeeperPort + bulkLoadFlag)
	}
}

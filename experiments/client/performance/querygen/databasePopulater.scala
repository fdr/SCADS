package edu.berkeley.cs.scads.querygen

//import edu.berkeley.cs.scads.model._
import org.apache.log4j._
import org.apache.log4j.Level._
//import edu.berkeley.cs.scads.thrift._
import edu.berkeley.cs.scads.piql._
import edu.berkeley.cs.scads.piql.parser._
import edu.berkeley.cs.scads.storage._
import piql._

object PopulateDatabase {
  	def main(args: Array[String]) {
		// Get cmd line args
		println("Data parameters:")
		val numUsers = System.getProperty("numUsers").toInt
		println("  numUsers=>" + System.getProperty("numUsers") + "<")
		val thoughtsPerUser = System.getProperty("thoughtsPerUser").toInt
		println("  thoughtsPerUser=>" + System.getProperty("thoughtsPerUser") + "<")
		val subsPerUser = System.getProperty("subsPerUser").toInt
		println("  subsPerUser=>" + System.getProperty("subsPerUser") + "<")
		val emailDupFactor = System.getProperty("emailDupFactor").toInt
		println("  emailDupFactor=>" + System.getProperty("emailDupFactor") + "<")
		val thoughtstreamLength = System.getProperty("thoughtstreamLength").toInt
		println("  thoughtstreamLength=>" + System.getProperty("thoughtstreamLength") + "<")
		val numHashTagsPerThought = System.getProperty("numHashTagsPerThought").toInt
		println("  numHashTagsPerThought=>" + numHashTagsPerThought + "<")
		val numDistinctHashTags = System.getProperty("numDistinctHashTags").toInt
		println("  numDistinctHashTags=>" + numDistinctHashTags + "<")
		val paramMapOutputFilename = System.getProperty("paramMapOutputFilename")
		println("  paramMapOutputFilename=>" + paramMapOutputFilename + "<")
		
		println("Cluster parameters:")
		val storageNodeServer = System.getProperty("storageNodeServer")
		println("  storageNodeServer=>" + storageNodeServer + "<")
		val zookeeperServerAndPort = System.getProperty("zookeeperServerAndPort")
		println("  zookeeperServerAndPort=>" + zookeeperServerAndPort + "<")
		
		
		// Deploy cluster
		/*
		implicit val env = new Environment
		env.session = new TrivialSession
		env.executor = new TrivialExecutor

		if (storageNodeServer == null) {
			println("Setting up cluster in process...")
			env.placement = new TestCluster
		} else {
			println("Setting up full cluster...")
			Queries.configureStorageEngine(new StorageNode(storageNodeServer, 9000))  // set responsibility policy
			env.placement = new ZooKeptCluster(zookeeperServerAndPort)
		}
		*/
		implicit val env = Configurator.configure(TestScalaEngine.cluster)
		
		println("Cluster ready.")
		
		
		// Populate database
		val dataManager = new SCADrDataManager
		val paramMap = dataManager.getParamMap(numUsers, emailDupFactor, thoughtstreamLength, numDistinctHashTags)
		dataManager.populateDB(paramMap, emailDupFactor, thoughtsPerUser, subsPerUser, numHashTagsPerThought)
		

		// Save paramMap
		dataManager.serializeParamMap(paramMap, paramMapOutputFilename)
		
		
		/*
		// Test:  read paramMap back in
		val testParamMap = dataManager.deserializeParamMap(paramMapOutputFilename)
		println(testParamMap("userByName")(0))
		*/
		
		
		System.exit(0)
		
	}
}

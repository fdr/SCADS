import edu.berkeley.cs.scads.model._
import edu.berkeley.cs.scads.thrift._

import org.apache.log4j._
import org.apache.log4j.Level._

import java.io._
import java.net._

import querygen._

import org.apache.commons.cli._

object WorkloadRunnerForEC2 {
  	def main(args: Array[String]) {
	
		val options = new Options();
		val queryLogger = Logger.getLogger("queryLogger")
		val opLogger = Logger.getLogger("scads.queryexecution.operators")
		val primitiveLogger = Logger.getLogger("scads.readpolicy.primitives")
		

		// Setup cmd line args
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
		options.addOption("storageNodeServer", true, "hostname of storage node server")
		options.addOption("zookeeperServerAndPort", true, "hostname & port of zookeeper server")
		options.addOption("duration", true, "duration of run (s)")
		options.addOption("warmupDuration", true, "duration of warmup (s)")
		
		// Get cmd line args
		val parser = new GnuParser();
		val cmd = parser.parse(options, args);
		
		// Data params
		val numUsers = cmd.getOptionValue("numUsers").toInt
		val thoughtsPerUser = cmd.getOptionValue("thoughtsPerUser").toInt
		val subsPerUser = cmd.getOptionValue("subsPerUser").toInt
		val emailDupFactor = cmd.getOptionValue("emailDupFactor").toInt
		val thoughtstreamLength = cmd.getOptionValue("thoughtstreamLength").toInt
		val numHashTagsPerThought = cmd.getOptionValue("numHashTagsPerThought").toInt
		val numDistinctHashTags = cmd.getOptionValue("numDistinctHashTags").toInt

		// Experiment params
		val numThreads = cmd.getOptionValue("numThreads").toInt
		val mixChoice = cmd.getOptionValue("mixChoice")
		val storageNodeServer = cmd.getOptionValue("storageNodeServer")
		val zookeeperServerAndPort = cmd.getOptionValue("zookeeperServerAndPort")
		val duration = cmd.getOptionValue("duration").toInt
		val warmupDuration = cmd.getOptionValue("warmupDuration").toInt

		// Deploy cluster
		implicit val env = new Environment
		env.session = new TrivialSession
		env.executor = new TrivialExecutor

		val n = new StorageNode(storageNodeServer, 9000)
		Queries.configureStorageEngine(n)  // set responsibility policy
		env.placement = new ZooKeptCluster(zookeeperServerAndPort)

		val paramMap = getParamMap(numUsers, emailDupFactor, thoughtstreamLength, numDistinctHashTags)

		/*
		val i = 1
		val u = new user
		u.name(paramMap("userByName")(i-1))
		u.password("secret")
		u.email(paramMap("userByEmail")(i % (numUsers/emailDupFactor)))
		queryLogger.info(u)
		u.save
		*/


		populateDB(paramMap, emailDupFactor, thoughtsPerUser, subsPerUser, numHashTagsPerThought)

		var mix:MixVector = null
		mixChoice match {
			case "userByName" => mix = WorkloadGenerators.mixUserByName
			case "userByEmail" => mix = WorkloadGenerators.mixUserByEmail
			case "thoughtstream" => mix = WorkloadGenerators.mixThoughtstream
			case "thoughtsByHashTag" => mix = WorkloadGenerators.mixThoughtsByHashTag
			case _ => mix = WorkloadGenerators.mixUserByName
		}
		
		// Warmup
		if (warmupDuration > 0) {
			queryLogger.info("Warming up jvm...")
			opLogger.info("Warming up jvm...")
			primitiveLogger.info("Warming up jvm...")

			val threads = (1 to numThreads).toList.map((id) => {	
				val agent = new WorkloadAgentForEC2(WorkloadGenerators.constantWorkload(mix, paramMap, numUsers, 1, List(warmupDuration*1000), env), id, env)
				new Thread(agent)
			})

			for(thread <- threads) thread.start
			for(thread <- threads) thread.join
		} else {
			queryLogger.info("Skipping warmup...")
			opLogger.info("Skipping warmup...")
			primitiveLogger.info("Skipping warmup...")
		}
		
		// Set up threads
		queryLogger.info("Starting run...")
		opLogger.info("Starting run...")
		primitiveLogger.info("Starting run...")

		val threads = (1 to numThreads).toList.map((id) => {	
			val agent = new WorkloadAgentForEC2(WorkloadGenerators.constantWorkload(mix, paramMap, numUsers, 1, List(duration*1000), env), id, env)
			new Thread(agent)
		})
		
		// Run the test
		for(thread <- threads) thread.start
		for(thread <- threads) thread.join

		System.exit(0)
  	}

	
	def populateDB(paramMap:Map[String, List[String]], emailDupFactor:Int, thoughtsPerUser:Int, 
		subsPerUser:Int, hashTagsPerThought:Int)(implicit env:Environment) = {
		val queryLogger = Logger.getLogger("queryLogger")
		/*
		queryLogger.info("Checking whether db is populated...")
		val nodes = env.placement.locate("ent_user", "user1")	// This assumes that db pop => I will have created the "ent_user" ns, and "user1" will have been added
		val rand = new scala.util.Random()
		val node = nodes(rand.nextInt(nodes.length))

		val sz = node.useConnection((c) => {
			c.count_set("ent_user", RangedPolicy.convert((null,null)).get(0))
		})
		*/

		//if (sz == 0) {
			queryLogger.info("DB is NOT populated yet.")
			queryLogger.info("Adding users & their thoughts (with hash tags)...")

			val numUsers = paramMap("userByName").length
			val numEmails = paramMap("userByEmail").length
			val thoughtstreamLength = paramMap("thoughtstream")(0)
			
			val hashTagGenerator = new SimpleHashTagGenerator(hashTagsPerThought, paramMap("thoughtsByHashTag"), env)
			//val gen = new SimpleThoughtGenerator(thoughtsPerUser, env)
			val thoughtGenerator = new SimpleThoughtGeneratorWithHashTags(thoughtsPerUser, hashTagGenerator, env)
			
			(1 to numUsers).foreach((i) => {
				// Create user
				val u = new user
				u.name(paramMap("userByName")(i-1))
				u.password("secret")
				u.email(paramMap("userByEmail")(i % (numUsers/emailDupFactor)))
				//queryLogger.info(u)
				u.save

				//gen.generateThoughts(paramMap("userByName")(i-1))
				thoughtGenerator.generateThoughts(paramMap("userByName")(i-1))
				
				
				if ((i % 100) == 0)
					queryLogger.info("Added " + i + " users...")
			})
			queryLogger.info("Users & thoughts added.")
			
			queryLogger.info("Adding subscriptions...")

			val subGen = new SimpleSubscriptionGenerator(paramMap("userByName"), subsPerUser, env)
			(1 to numUsers).foreach((i) => {
				subGen.generateSubscriptions(paramMap("userByName")(i-1))
			})

			queryLogger.info("Finished adding subscriptions.")
			
		//}
		queryLogger.info("DB is populated.")	
		
	}
	
	
	
	
	def getParamMap(numUsers:Int, emailDupFactor:Int, numThoughts:Int, 
		numDistinctHashTags:Int): Map[String, List[String]] = {
		val queryLogger = Logger.getLogger("queryLogger")
		// Usernames
		var usernames:List[String] = Nil
		(1 to numUsers).foreach((i) => {
			usernames = ("user" + i).toString :: usernames
		})
		usernames = usernames.reverse
		
		// Emails
		// emailDupFactor = how many users share each email addr
		// Useful for "userByEmail" query
		var emails:List[String] = Nil
		(1 to (numUsers/emailDupFactor)).foreach((i) => {
			emails = ("user" + i + "@berkeley.edu") :: emails
		})
		emails = emails.reverse
		
		// Hash Tags
		var hashTags:List[String] = Nil
		(1 to numDistinctHashTags).foreach((i) => {
			hashTags = ("tag" + i).toString :: hashTags
		})
		hashTags = hashTags.reverse
		
		val paramMap = Map("userByName"->usernames, "userByEmail"->emails, "thoughtstream"->List(numThoughts.toString),
			"thoughtsByHashTag"->hashTags)	
			// "thoughtstream" reuses "usernames"; "thoughtsByHashTag" reuses "numThoughts", since this is related to how many can 
			// be displayed.  Could update in future.
		paramMap
	}

}

class WorkloadAgentForEC2(workload:WorkloadDescription, userID:Int, env:Environment) extends Runnable {

	/*
	var threadlogf: FileWriter = null
	def threadlog(line: String) {
		threadlogf.write(new java.util.Date() + ": " + line+"\n")
		threadlogf.flush
	}
	*/


	def run() = {
		val queryLogger = Logger.getLogger("queryLogger")
		
		var startt = System.nanoTime()
		var startt_ms = System.currentTimeMillis()
		var endt = System.nanoTime()
		var endt_ms = System.currentTimeMillis()
		var latency = endt - startt
		
		val thread_name = Thread.currentThread().getName()

		/*
		(new File(path + "logs/")).mkdirs()
		threadlogf = new FileWriter( new java.io.File(path+"logs/"+thread_name+".log"), true )
		threadlog("Thread Name, Query Type, Query Param, Query Result, Start (ms), End (ms), Latency (ms)")
		*/
		//queryLogger.info("Thread Name, Query Type, Query Param, Query Result, Start (ms), End (ms), Latency (ms)")
		
		val thinkTime = 10
		
		var currentIntervalDescriptionI = 0
		var currentIntervalDescription = workload.workload(currentIntervalDescriptionI)
		val workloadStart = System.currentTimeMillis()
		var nextIntervalTime = workloadStart + currentIntervalDescription.duration

		var requestI = 0;
		var running = true;

		while (running) {

			if (currentIntervalDescription.numberOfActiveUsers >= userID) {
				//threadlog("ACTIVE")
				//queryLogger.info("ACTIVE")
				// I'm active, send a request
				requestI += 1

				// create the request
				val query = currentIntervalDescription.queryGenerator.generateQuery

				try {
					//println(new java.util.Date() + ": " + thread_name + " starting: " + query.toString)
					startt = System.nanoTime()
					query.execute
					endt = System.nanoTime()
					
					latency = endt - startt
					
					//println(new java.util.Date() + ": " + thread_name + " executed: " + query.toString + ", start=" + (startt/1000000.0) + ", end=" + (endt/1000000.0) + ", latency=" + (latency/1000000.0))
					//println(new java.util.Date() + ": " + thread_name + " ending: " + query.toString)
					
					queryLogger.info(query.toString + ", start=" + (startt/1000000.0) + ", end=" + (endt/1000000.0) + ", latency=" + (latency/1000000.0))
					
				} catch {
					//case e: Exception => threadlog("got an exception. \n"+stack2string(e))
					case e: Exception => queryLogger.info("got an exception. \n"+stack2string(e))
				}

				//threadlog("thinking: "+workload.thinkTimeMean)
				//queryLogger.info("thinking: "+workload.thinkTimeMean)
				Thread.sleep(workload.thinkTimeMean)

			} else {
				// I'm inactive, sleep for a while
				//threadlog("PASSIVE, SLEEPING 1000ms")
				//queryLogger.info("PASSIVE, SLEEPING 1000ms")
				Thread.sleep(1000)
			}

			// check if time for next workoad interval
			val currentTime = System.currentTimeMillis()
			while ( currentTime > nextIntervalTime && running ) {
				currentIntervalDescriptionI += 1
				//println("interval "+currentIntervalDescriptionI)
				if (currentIntervalDescriptionI < workload.workload.length) {
					currentIntervalDescription = workload.workload(currentIntervalDescriptionI)
					nextIntervalTime += currentIntervalDescription.duration
					//threadlog("switching to workload interval #"+currentIntervalDescriptionI+"/"+workload.workload.length+" @ "+new java.util.Date() )
					queryLogger.info("switching to workload interval #"+currentIntervalDescriptionI+"/"+workload.workload.length+" @ "+new java.util.Date() )
				} else
					running = false
			}

		}

		queryLogger.info("done")
		
	}
	
	def stack2string(e:Exception):String = {
	    val sw = new StringWriter()
	    val pw = new PrintWriter(sw)
	    e.printStackTrace(pw)
	    sw.toString()
  	}
}

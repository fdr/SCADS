import edu.berkeley.cs.scads.model._
import edu.berkeley.cs.scads.thrift._

import org.apache.log4j._
import org.apache.log4j.Level._

import java.io._
import java.net._

import querygen._


object WorkloadRunner {
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

		println("Experiment parameters:")
		val numThreads = System.getProperty("numThreads").toInt
		println("  numThreads=>" + System.getProperty("numThreads") + "<")
		val mixChoice = System.getProperty("mixChoice")
		println("  mixChoice=>" + mixChoice + "<")
		val storageNodeServer = System.getProperty("storageNodeServer")
		println("  storageNodeServer=>" + storageNodeServer + "<")
		val zookeeperServerAndPort = System.getProperty("zookeeperServerAndPort")
		println("  zookeeperServerAndPort=>" + zookeeperServerAndPort + "<")

		/*
		val minUserId = System.getProperty("minUserId").toInt		// could phase out
		println("minUserId=>"+System.getProperty("minUserId")+"<")
		val maxUserId = System.getProperty("maxUserId").toInt
		println("maxUserId=>"+System.getProperty("maxUserId")+"<")
		*/

		// Disallowed for now... need to fix so it's easier to isolate each interval's corresponding queries & ops
		/*
		val nIntervals = System.getProperty("nIntervals").toInt		// could phase out
		println("nIntervals=>"+System.getProperty("nIntervals")+"<")
		*/

		val duration = System.getProperty("duration").toInt
		println("  duration=>" + System.getProperty("duration") + "s<")
		var warmupDuration = System.getProperty("warmupDuration").toInt
		println("  warmupDuration=>" + warmupDuration + "s<")
		val logPath = System.getProperty("logPath")
		println("  logPath=>" + logPath + "<")

		// Deploy cluster
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
		println("Cluster ready.")

		val paramMap = getParamMap(numUsers, emailDupFactor, thoughtstreamLength)
		populateDB(paramMap, emailDupFactor, thoughtsPerUser, subsPerUser)

		var mix:MixVector = null
		mixChoice match {
			case "userByName" => mix = WorkloadGenerators.mixUserByName
			case "userByEmail" => mix = WorkloadGenerators.mixUserByEmail
			case "thoughtstream" => mix = WorkloadGenerators.mixThoughtstream
			case _ => mix = WorkloadGenerators.mixUserByName
		}
		
		// Warmup
		if (warmupDuration > 0) {
			println("Warming up jvm...")
			//val threads = (minUserId to maxUserId).toList.map((id) => {
			val threads = (1 to numThreads).toList.map((id) => {	
				//val agent = new WorkloadAgent(getFlatWorkloadDescriptionForUserByEmail(maxUserId, nIntervals, warmupDuration, emails, env), logPath + "warmup/", id, env)
				val agent = new WorkloadAgent(WorkloadGenerators.constantWorkload(mix, paramMap, numUsers, 1, List(warmupDuration*1000), env), logPath + "warmup/", id, env)
				new Thread(agent)
			})

			for(thread <- threads) thread.start
			for(thread <- threads) thread.join
		} else
			println("Skipping warmup...")
		
		// Set up threads
		println("Starting run...")
		//val threads = (minUserId to maxUserId).toList.map((id) => {
		val threads = (1 to numThreads).toList.map((id) => {	
			// For now, disallow multiple intervals.  If I decide to phase this back in, I need to insert a pause b/t the intervals (like around 10s long) 
			// so it's easier to handle the overlap.
			/*
			if (nIntervals == 1) {
				val agent = new WorkloadAgent(getFlatWorkloadDescriptionForUserByEmail(maxUserId, nIntervals, duration, emails, env), logPath, id, env)
				new Thread(agent)
			} else {
				val agent = new WorkloadAgent(getStepWorkloadDescriptionForUserByEmail(minUserId, maxUserId, nIntervals, duration, emails, env), logPath, id, env)
				new Thread(agent)
			}
			*/
			val agent = new WorkloadAgent(WorkloadGenerators.constantWorkload(mix, paramMap, numUsers, 1, List(duration*1000), env), logPath + "run/", id, env)
			new Thread(agent)
		})
		
		// Run the test
		for(thread <- threads) thread.start
		for(thread <- threads) thread.join

		System.exit(0)
  	}

	def populateDB(paramMap:Map[String, List[String]], emailDupFactor:Int, thoughtsPerUser:Int, subsPerUser:Int)(implicit env:Environment) = {
		println("Checking whether db is populated...")
		val nodes = env.placement.locate("ent_user", "user1")	// This assumes that db pop => I will have created the "ent_user" ns, and "user1" will have been added
		val rand = new scala.util.Random()
		val node = nodes(rand.nextInt(nodes.length))

		val sz = node.useConnection((c) => {
			c.count_set("ent_user", RangedPolicy.convert((null,null)).get(0))
		})

		if (sz == 0) {
			println("DB is NOT populated yet.")
			println("Adding users & their thoughts...")

			val numUsers = paramMap("userByName").length
			val numEmails = paramMap("userByEmail").length
			val thoughtstreamLength = paramMap("thoughtstream")(0)
			
			val gen = new SimpleThoughtGenerator(thoughtsPerUser, env)
			(1 to numUsers).foreach((i) => {
				// Create user
				val u = new user
				u.name(paramMap("userByName")(i-1))
				u.password("secret")
				u.email(paramMap("userByEmail")(i % (numUsers/emailDupFactor)))
				u.save

				gen.generateThoughts(paramMap("userByName")(i-1))
			})
			println("Users & thoughts added.")
			
			println("Adding subscriptions...")

			val subGen = new SimpleSubscriptionGenerator(paramMap("userByName"), subsPerUser, env)
			(1 to numUsers).foreach((i) => {
				subGen.generateSubscriptions(paramMap("userByName")(i-1))
			})

			println("Finished adding subscriptions.")
			
		}
		println("DB is populated now.")	
		
	}
	
	def getParamMap(numUsers:Int, emailDupFactor:Int, numThoughts:Int): Map[String, List[String]] = {
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
		
		val paramMap = Map("userByName"->usernames, "userByEmail"->emails, "thoughtstream"->List(numThoughts.toString))	// "thoughtstream" also reuses "usernames"
		paramMap
	}
}

// "path" ends in "/"
class WorkloadAgent(workload:WorkloadDescription, path:String, userID:Int, env:Environment) extends Runnable {

	var threadlogf: FileWriter = null
	def threadlog(line: String) {
		threadlogf.write(new java.util.Date() + ": " + line+"\n")
		threadlogf.flush
	}


	def run() = {
		var startt = System.nanoTime()
		var startt_ms = System.currentTimeMillis()
		var endt = System.nanoTime()
		var endt_ms = System.currentTimeMillis()
		var latency = endt - startt
		
		val thread_name = Thread.currentThread().getName()

		(new File(path + "logs/")).mkdirs()
		threadlogf = new FileWriter( new java.io.File(path+"logs/"+thread_name+".log"), true )
		threadlog("Thread Name, Query Type, Query Param, Query Result, Start (ms), End (ms), Latency (ms)")
		
		val thinkTime = 10
		
		var currentIntervalDescriptionI = 0
		var currentIntervalDescription = workload.workload(currentIntervalDescriptionI)
		val workloadStart = System.currentTimeMillis()
		var nextIntervalTime = workloadStart + currentIntervalDescription.duration

		var requestI = 0;
		var running = true;

		while (running) {

			if (currentIntervalDescription.numberOfActiveUsers >= userID) {
				threadlog("ACTIVE")
				// I'm active, send a request
				requestI += 1

				// create the request
				val query = currentIntervalDescription.queryGenerator.generateQuery

				try {
					println(new java.util.Date() + ": " + thread_name + " starting: " + query.toString)

					startt = System.nanoTime()
					query.execute
					endt = System.nanoTime()
					
					latency = endt - startt
					
					println(new java.util.Date() + ": " + thread_name + " executed: " + query.toString + ", start=" + (startt/1000000.0) + ", end=" + (endt/1000000.0) + ", latency=" + (latency/1000000.0))
					println(new java.util.Date() + ": " + thread_name + " ending: " + query.toString)
					
					threadlog(thread_name + " executed: " + query.toString + ", start=" + (startt/1000000.0) + ", end=" + (endt/1000000.0) + ", latency=" + (latency/1000000.0))
					
				} catch {
					case e: Exception => threadlog("got an exception. \n"+stack2string(e))
				}

				threadlog("thinking: "+workload.thinkTimeMean)
				Thread.sleep(workload.thinkTimeMean)

			} else {
				// I'm inactive, sleep for a while
				threadlog("PASSIVE, SLEEPING 1000ms")
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
					threadlog("switching to workload interval #"+currentIntervalDescriptionI+"/"+workload.workload.length+" @ "+new java.util.Date() )
				} else
					running = false
			}

		}

		threadlog("done")
		threadlogf.close()
		
	}
	
	def stack2string(e:Exception):String = {
	    val sw = new StringWriter()
	    val pw = new PrintWriter(sw)
	    e.printStackTrace(pw)
	    sw.toString()
  	}
}

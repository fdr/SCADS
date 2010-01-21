//package performance
//package querygen

/*
import edu.berkeley.xtrace._
import edu.berkeley.cs.scads.thrift._
import edu.berkeley.cs.scads.nodes._
import edu.berkeley.cs.scads.keys._
import edu.berkeley.cs.scads.placement._
import edu.berkeley.cs.scads.client._
import org.apache.thrift.transport.{TFramedTransport, TSocket}
import org.apache.thrift.protocol.{TBinaryProtocol,XtBinaryProtocol}
*/

import edu.berkeley.cs.scads.model._
import edu.berkeley.cs.scads.thrift._

import org.apache.log4j._
import org.apache.log4j.Level._

import java.io._
import java.net._

import querygen._


object WorkloadRunner {
  	def main(args: Array[String]) {
	
		// Kristal's code to generate queries:
		// Get system properties (ie, cmd line args)

		val dbSize = System.getProperty("dbSize").toInt
		println("dbSize=>" + System.getProperty("dbSize") + "<")
		val minUserId = System.getProperty("minUserId").toInt
		println("minUserId=>"+System.getProperty("minUserId")+"<")
		val maxUserId = System.getProperty("maxUserId").toInt
		println("maxUserId=>"+System.getProperty("maxUserId")+"<")
		val nIntervals = System.getProperty("nIntervals").toInt
		println("nIntervals=>"+System.getProperty("nIntervals")+"<")
		val duration = System.getProperty("duration").toInt
		println("duration=>" + System.getProperty("duration") + "<")
		val logPath = System.getProperty("logPath")
		println("logPath=>" + logPath + "<")
		val warmup = System.getProperty("warmup")	// true => warm up the jvm by having a 5-min test run first
		println("warmup=>" + warmup + "<")


		/*
		// Set up cluster (in process/locally)
		implicit val env = new Environment
		env.placement = new TestCluster
		env.session = new TrivialSession
		env.executor = new TrivialExecutor
		*/

		// Configure storage engine
		Queries.configureStorageEngine(new StorageNode("r21", 9000))  // set responsibility policy -- r21 instead of ip

		// Testing ZooKeptCluster
		implicit val env = new Environment
		//env.placement = new ZooKeptCluster("r13:3000")
		env.placement = new ZooKeptCluster("r15:3000") // need to give this via cmd line arg, not hard coding, so i don't have to recompile if it changes
		env.session = new TrivialSession
		env.executor = new TrivialExecutor

		
		// Populate the db -- in the future, move this to a function
		/*
		(1 to 1000).foreach((i) => {
			val u = new user
			u.name("user" + i)
			u.password("secret")
			u.email("user" + i + "@test.com")
			u.save
		})
		*/
		//val emails = populateDBwithDupEmails(dbSize, 20 /*hard-coded*/, env)
		
		val numUsers = 1000 // hard-coded
		val emailDup = 20 // hard-coded

		var emails:List[String] = Nil
		(1 to (numUsers/emailDup)).foreach((i) => {
			emails = ("user" + i + "@berkeley.edu") :: emails
		})
		emails = emails.reverse
		
		// better way would be to check if the db is already populated; if no, populate
		if (dbSize > 0) {
			(1 to numUsers).foreach((i) => {
				val u = new user
				u.name("user" + i)
				u.password("secret")
				u.email(emails(i % (numUsers/emailDup)))
				u.save
			})
		}
		
		if (warmup == "true") {
			println("warming up jvm...")
			val warmupDuration = 5*60 // 5 min
			val threads = (minUserId to maxUserId).toList.map((id) => {
				val agent = new WorkloadAgent(getFlatWorkloadDescriptionForUserByEmail(maxUserId, nIntervals, warmupDuration, emails, env), logPath + "warmup/", id, env)
				new Thread(agent)
			})

			for(thread <- threads) thread.start
			for(thread <- threads) thread.join
		}
		
		/*
		// Set up workload -- file must be generated beforehand
		val workloadFile = System.getProperty("workload")
		val workload = WorkloadDescription.deserialize(workloadFile)
		*/
		// Set up threads
		// How many?  I have no idea.  Start out just exploring how it seems to work with a few diff #s (eg, 10 threads, 100 threads, 1000 threads)
		println("starting run...")
		val threads = (minUserId to maxUserId).toList.map((id) => {
			//val agent = new WorkloadAgent(new SCADSClient(host,port), workload, id)  // Change this line -- I'll have a different WorkloadAgent, with different params
			//val agent = new WorkloadAgent("/Users/ksauer/Desktop/", env)
			//val agent = new WorkloadAgent(getWorkloadDescription(minUserId, maxUserId, nIntervals, env), "/Users/ksauer/Desktop/", id, env)
			if (nIntervals == 1) {
				//val agent = new WorkloadAgent(getFlatWorkloadDescriptionForUserByEmail(maxUserId, nIntervals, duration, emails, env), "/Users/ksauer/Desktop/", id, env)
				val agent = new WorkloadAgent(getFlatWorkloadDescriptionForUserByEmail(maxUserId, nIntervals, duration, emails, env), logPath, id, env)
				new Thread(agent)
			} else {
				//val agent = new WorkloadAgent(getStepWorkloadDescriptionForUserByEmail(minUserId, maxUserId, nIntervals, duration, emails, env), "/Users/ksauer/Desktop/", id, env)
				val agent = new WorkloadAgent(getStepWorkloadDescriptionForUserByEmail(minUserId, maxUserId, nIntervals, duration, emails, env), logPath, id, env)
				new Thread(agent)
			}
		})
		
		// Run the test
		for(thread <- threads) thread.start
		for(thread <- threads) thread.join
		
		/*
		val u1 = new user
		u1.name("marmbrus")
		u1.password("pass")
		u1.email("marmbrus@berkeley.edu")
		u1.save
		
		// Issue a query and print the result
		val res = Queries.userByName("marmbrus").apply(0).name
		println()
		println("ANS:")
		println(res)
		println()
		*/
		
		System.exit(0)
  	}

/*
	def populateDBwithDupEmails(numUsers:Int, emailDup:Int, env: Environment): List[String] = {
		// emailDup => how many users will share each email

		var emails:List[String] = Nil
		(1 to (numUsers/emailDup)).foreach((i) => {
			emails = ("user" + i + "@berkeley.edu") :: emails
		})
		emails = emails.reverse

		(1 to numUsers).foreach((i) => {
			val u = new user
			u.name("user" + i)
			u.password("secret")
			u.email(emails(i % (numUsers/emailDup)))
			u.save
		})
		
		emails
	}
	*/

	// constructs a really basic paramMap
	// interval duration is hard-coded at 10s
	def getWorkloadDescription(minUsers:Int, maxUsers:Int, nIntervals:Int, env:Environment): WorkloadDescription = {
		// mix
		val mix100 = new MixVector(Map("userByName"->1.0,"b"->0.0,"c"->0.0))

		// parameters
		var possibleParams = List("user1")
		(2 to 1000).foreach((i) => {
			possibleParams = ("user" + i) :: possibleParams
		})
		possibleParams = possibleParams.reverse

		val paramMap = Map("userByName"->possibleParams, "b"->List("b"), "c"->List("c"))

		val durations = List.make(nIntervals, 10*1000)		// NOTE THIS IS HARD-CODED at 10s intervals!

		val wd = WorkloadGenerators.stepWorkload(mix100, paramMap, minUsers, maxUsers, nIntervals, durations, env)
		wd
	}

	// lets you create a paramMap and pass it in
	// "duration" is each interval's duration, in seconds (pass it in)
	def getWorkloadDescription(minUsers:Int, maxUsers:Int, nIntervals:Int, duration:Int, mix:MixVector, paramMap:Map[String, List[String]], env:Environment): WorkloadDescription = {
		val durations = List.make(nIntervals, duration*1000)

		val wd = WorkloadGenerators.stepWorkload(mix, paramMap, minUsers, maxUsers, nIntervals, durations, env)
		wd
	}
	
	def getStepWorkloadDescriptionForUserByEmail(minUsers:Int, maxUsers:Int, nIntervals:Int, duration:Int, emails:List[String], env:Environment): WorkloadDescription = {
		val mix = new MixVector(Map("userByEmail"->1.0, "userByName"->0.0))
		val paramMap = Map("userByEmail"->emails, "userByName"->List("name"))
		
		val wd = getWorkloadDescription(minUsers, maxUsers, nIntervals, duration, mix, paramMap, env)
		wd
	}
	
	def getFlatWorkloadDescriptionForUserByEmail(nUsers:Int, nIntervals:Int, duration:Int, emails:List[String], env:Environment): WorkloadDescription = {
		val mix = new MixVector(Map("userByEmail"->1.0, "userByName"->0.0))
		val paramMap = Map("userByEmail"->emails, "userByName"->List("name"))
		val durations = List.make(nIntervals, duration*1000)  // duration is in s
		
		val wd = WorkloadGenerators.constantWorkload(mix, paramMap, nUsers, nIntervals, durations, env)
		wd
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
		//threadlog("starting workload generation. thread:"+thread_name+"  userID="+userID)
		threadlog("Thread Name, Query Type, Query Param, Query Result, Start (ms), End (ms), Latency (ms)")
		
		val thinkTime = 10
		
		/*
		try {

			// Try having each thread send a query to the db
			val rdm = new RandomUser
			rdm.setNumUsers(1000)
			
			// Redundant, but just to get correct type for "name"
			var idx = rdm.getRandomUser
			var param = "user" + idx
			var name = Queries.userByName(param).apply(0).name
			
			(1 to 5).foreach((i) => {
				// Set up request
				idx = rdm.getRandomUser
				param = "user" + idx

				startt = System.nanoTime()
				startt_ms = System.currentTimeMillis()

				name = Queries.userByName(param).apply(0).name
				//threadlog("User " + idx + ": " + Queries.userByName("user" + idx).apply(0).name)

				endt = System.nanoTime()
				endt_ms = System.currentTimeMillis()
				latency = endt-startt

				threadlog(thread_name +"," + "userByName" + "," + param + "," + name + "," + startt_ms + "," + endt_ms + "," + (latency/1000000.0))

				threadlog("thinking: "+thinkTime + "\n")
				Thread.sleep(thinkTime)
			})
		} catch {
			case e: Exception => threadlog("got an exception. \n"+stack2string(e))
		}
		*/
		
		var currentIntervalDescriptionI = 0
		var currentIntervalDescription = workload.workload(currentIntervalDescriptionI)
		val workloadStart = System.currentTimeMillis()
		var nextIntervalTime = workloadStart + currentIntervalDescription.duration

		//var result = new scala.collection.mutable.ListBuffer[String]() // "request,threads,types,start,end,latency\n"
		var requestI = 0;
		var running = true;

		while (running) {

			if (currentIntervalDescription.numberOfActiveUsers >= userID) {
				threadlog("ACTIVE")
				// I'm active, send a request
				requestI += 1

				// create the request
				//val request = currentIntervalDescription.requestGenerator.generateRequest(client, System.currentTimeMillis-workloadStart)
				val query = currentIntervalDescription.queryGenerator.generateQuery
				/*
				val reportProb = if (request.reqType=="get") getReportProbability
								else if (request.reqType=="put") putReportProbability
								else 0.0
				severity = if (WorkloadDescription.rnd.nextDouble < reportProb) {1} else {6}

				XTraceContext.startTraceSeverity(thread_name,"Initiated: LocalRequest",severity,"RequestID: "+requestI)
				*/

				try {
					/*
					startt = System.nanoTime()
					startt_ms = System.currentTimeMillis()
					
					//request.execute
					query.execute
					
					endt = System.nanoTime()
					endt_ms = System.currentTimeMillis()
					latency = endt-startt
					//result += (localIP+","+thread_name+","+requestI+","+request.reqType+","+startt_ms+","+endt_ms+","+(latency/1000000.0)+"\n")
					//threadlog("executed: "+request.toString+"    latency="+(latency/1000000.0))
					threadlog(thread_name + " executed: " + query.toString + ", start=" + startt_ms + ", end=" + endt_ms + ", latency=" + (latency/1000000.0))
					//threadlog("executed: " + query.toString + ", start=" + startt_ms + ", end=" + endt_ms + ", latency=" + (latency/1000000.0))
					//threadlog(thread_name +"," + "userByName" + "," + param + "," + name + "," + startt_ms + "," + endt_ms + "," + (latency/1000000.0))
					*/

					// Just use nanoTime; don't make the extra syscall
					startt = System.nanoTime()
					query.execute
					endt = System.nanoTime()
					
					latency = endt - startt
					
					threadlog(thread_name + " executed: " + query.toString + ", start=" + (startt/1000000.0) + ", end=" + (endt/1000000.0) + ", latency=" + (latency/1000000.0))
					
				} catch {
					case e: Exception => threadlog("got an exception. \n"+stack2string(e))
				}

				//XTraceContext.clearThreadContext()

				// periodically flush log to disk and clear result list
				/*
				if (requestI%5000==0) {
					if (logwriter != null) {
						logwriter.write(result.mkString)
						logwriter.flush()
					}
					//result = result.remove(p=>true)
					result = new scala.collection.mutable.ListBuffer[String]()
				}
				*/

				threadlog("thinking: "+workload.thinkTimeMean)
				Thread.sleep(workload.thinkTimeMean)

			} else {
				// I'm inactive, sleep for a while
				//println("inactive, sleeping 1 second")
				threadlog("PASSIVE, SLEEPING 1000ms")
				Thread.sleep(1000)
			}

			// check if time for next workoad interval
			val currentTime = System.currentTimeMillis()
			while ( currentTime > nextIntervalTime && running ) {
				currentIntervalDescriptionI += 1
				println("interval "+currentIntervalDescriptionI)
				if (currentIntervalDescriptionI < workload.workload.length) {
					currentIntervalDescription = workload.workload(currentIntervalDescriptionI)
					nextIntervalTime += currentIntervalDescription.duration
					threadlog("switching to workload interval #"+currentIntervalDescriptionI+"/"+workload.workload.length+" @ "+new java.util.Date() )
				} else
					running = false
			}

		}

		threadlog("done")

		//if (logwriter != null) { logwriter.write(result.mkString); logwriter.flush(); logwriter.close() }
		threadlogf.close()
		
	}
	
	def stack2string(e:Exception):String = {
	    val sw = new StringWriter()
	    val pw = new PrintWriter(sw)
	    e.printStackTrace(pw)
	    sw.toString()
  	}
}

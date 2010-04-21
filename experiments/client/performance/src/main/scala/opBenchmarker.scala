import edu.berkeley.cs.scads.model._
import org.apache.log4j._
import org.apache.log4j.Level._
import edu.berkeley.cs.scads.thrift._

object BenchmarkOps {
  	def main(args: Array[String]) {
		// Cmd line args
		println("Experiment:")
		val whichOp = System.getProperty("whichOp").toInt			// only one at a time => take advantage of ||ism using EC2
		println("whichOp=>" + whichOp + "<")
		val numThreads = System.getProperty("numThreads").toInt		// only generate ops from a single machine
		println("numThreads=>" + numThreads + "<")
		val warmupDuration = System.getProperty("warmupDuration").toInt
		println("warmupDuration(s)=>" + warmupDuration + "<")
		val runDuration = System.getProperty("runDuration").toInt
		println("runDuration(s)=>" + runDuration + "<")
		
		// # data items
		println("# data items:")
		val minItems = System.getProperty("minItems").toInt
		println("minItems=>" + minItems + "<")
		val maxItems = System.getProperty("maxItems").toInt
		println("maxItems=>" + maxItems + "<")
		val itemsInc = System.getProperty("itemsInc").toInt
		println("itemsInc=>" + itemsInc + "<")

		// data size
		println("Data size:")
		val minChars = System.getProperty("minChars").toInt
		println("minChars=>" + minChars + "<")
		val maxChars = System.getProperty("maxChars").toInt
		println("maxChars=>" + maxChars + "<")
		val charsInc = System.getProperty("charsInc").toInt
		println("charsInc=>" + charsInc + "<")
		
		// setup
		println("Setup:")
		val zookeeperServerAndPort = System.getProperty("zookeeperServerAndPort")
		println("  zookeeperServerAndPort=>" + zookeeperServerAndPort + "<")
		val storageNodeServer = System.getProperty("storageNodeServer")
		println("  storageNodeServer=>" + storageNodeServer + "<")
		
		
		// Deploy cluster
		implicit val env = new Environment
		env.session = new TrivialSession
		env.executor = new TrivialExecutor

		/*
		if (storageNodeServer == null) {
			println("Setting up cluster in process...")
			env.placement = new TestCluster
		} else {
			println("Setting up full cluster...")
			val n = new StorageNode(storageNodeServer, 9000)
			Queries.configureStorageEngine(n)  // set responsibility policy
			env.placement = new ZooKeptCluster(zookeeperServerAndPort)
		}
		println("Cluster ready.")
		*/
		println("Setting up full cluster...")
		val n = new StorageNode(storageNodeServer, 9000)
		Queries.configureStorageEngine(n)  // set responsibility policy
		env.placement = new ZooKeptCluster(zookeeperServerAndPort)
		
		
		// Benchmark requested op
		val itemsRange = minItems to maxItems by itemsInc
		val charsRange = minChars to maxChars by charsInc
		
		println("Data collection length (min): " + ((warmupDuration + runDuration * itemsRange.length * itemsRange.length * charsRange.length * charsRange.length)/60))
		
		var warmupDone = false
		
		charsRange.foreach((aSize) => {
			charsRange.foreach((bSize) => {
				println("New data size pair:  size(A) = " + aSize + ", size(B) = " + bSize)
				
				// Delete previous data
				val allKeys = RangedPolicy.convert((null, null)).get(0)
				n.useConnection((c) => {
					c.remove_set("ent_A", allKeys)
				})
				n.useConnection((c) => {
					c.remove_set("ent_B", allKeys)
				})

				// Confirm that data has been deleted
				val szA = n.useConnection((c) => {
					c.count_set("ent_A", allKeys)
				})
				val szB = n.useConnection((c) => {
					c.count_set("ent_B", allKeys)
				})

				if (szA == 0) {
					println("ent_A is empty.")
				}
				if (szB == 0) {
					println("ent_B is empty.")
				}

				
				// Generate data for requested op
				PerOpDataGen.genData(whichOp, aSize, bSize, maxItems)
				
				// For each data size, get a bunch of measurements (varying the #s of A & B)
				itemsRange.foreach((numA) => {
					itemsRange.foreach((numB) => {

						// Warmup (only the current op, and only the first time through -- just need to hit all the code paths)
						if (warmupDuration > 0 & !warmupDone) {
							println("Warming up jvm...")
							val threads = (1 to numThreads).toList.map((id) => {	
								val agent = new BenchmarkAgent(whichOp, s_to_ms(warmupDuration), aSize, numA, numB, maxItems)
								new Thread(agent)
							})

							for(thread <- threads) thread.start
							for(thread <- threads) thread.join

							warmupDone = true
						} else
							println("Skipping warmup...")
						

						// Run
						println("Starting run with #A=" + numA + ", #B=" + numB + "...")
						val threads = (1 to numThreads).toList.map((id) => {	
							val agent = new BenchmarkAgent(whichOp, s_to_ms(runDuration), aSize, numA, numB, maxItems)
							new Thread(agent)
						})

						// Run the test
						for(thread <- threads) thread.start
						for(thread <- threads) thread.join
					})
				})
			})
		})
	}

	// Convert seconds to ms
	def s_to_ms(s_val:Int):Int = {
		s_val * 1000
	}

}


class BenchmarkAgent(opType:Int, duration_ms:Int, aSize:Int, numA:Int, numB:Int, maxItems:Int)(implicit env:Environment) extends Runnable {
	def run = {
		val startTime_ms = System.currentTimeMillis()
		val thinkTime_ms = 10
		
		var currentTime_ms = System.currentTimeMillis()
		while (currentTime_ms < (startTime_ms + duration_ms.longValue())) {
			PerOpBenchmarker.benchmarkOp(opType, aSize, numA, numB, maxItems)
			
			println(Thread.currentThread().getName() + " is thinking for " + thinkTime_ms + " ms")
			Thread.sleep(thinkTime_ms)
			
			currentTime_ms = System.currentTimeMillis()
		}
		
	}
}


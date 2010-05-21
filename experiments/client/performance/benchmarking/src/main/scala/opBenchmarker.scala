import edu.berkeley.cs.scads.model._
import org.apache.log4j._
import org.apache.log4j.Level._
import edu.berkeley.cs.scads.thrift._

import org.apache.commons.cli.Options
import org.apache.commons.cli.GnuParser
import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.HelpFormatter


object BenchmarkOps {
  	def main(args: Array[String]) {
		val opLogger = Logger.getLogger("scads.queryexecution.operators")
		val primitiveLogger = Logger.getLogger("scads.readpolicy.primitives")
		
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
		options.addOption("zookeeperServerAndPort", true, "zookeeper server's hostname & port")
		options.addOption("storageNodeServer", true, "storage node server's hostname")

		val parser = new GnuParser();
		val cmd = parser.parse(options, args);

		// Get args from cmd line
		// Experiment
		val whichOp = cmd.getOptionValue("whichOp").toInt
		opLogger.info("Benchmarking op " + whichOp + "...")
		primitiveLogger.info("Benchmarking op " + whichOp + "...")
		
		val oneBin = cmd.getOptionValue("oneBin").toBoolean
		opLogger.info("One bin? " + oneBin)
		primitiveLogger.info("One bin? " + oneBin)

		val numThreads = cmd.getOptionValue("numThreads").toInt
		opLogger.info("Using " + numThreads + " threads.")
		primitiveLogger.info("Using " + numThreads + " threads.")
		
		val warmupDuration = cmd.getOptionValue("warmupDuration").toInt
		opLogger.info("Warmup duration (s):  " + warmupDuration)
		primitiveLogger.info("Warmup duration (s):  " + warmupDuration)
		
		val runDuration = cmd.getOptionValue("runDuration").toInt
		opLogger.info("Run duration (s):  " + runDuration)
		primitiveLogger.info("Run duration (s):  " + runDuration)
		
		// # data items:  A
		val minItemsA = cmd.getOptionValue("minItemsA").toInt
		opLogger.info("Min items (A): " + minItemsA)
		primitiveLogger.info("Min items (A): " + minItemsA)
		
		val maxItemsA = cmd.getOptionValue("maxItemsA").toInt
		opLogger.info("Max items (A): " + maxItemsA)
		primitiveLogger.info("Max items (A): " + maxItemsA)

		val itemsIncA = cmd.getOptionValue("itemsIncA").toInt
		opLogger.info("Items inc (A): " + itemsIncA)
		primitiveLogger.info("Items inc (A): " + itemsIncA)			
		
		// # data items:  B
		val minItemsB = cmd.getOptionValue("minItemsB").toInt
		opLogger.info("Min items (B): " + minItemsB)
		primitiveLogger.info("Min items (B): " + minItemsB)

		val maxItemsB = cmd.getOptionValue("maxItemsB").toInt
		opLogger.info("Max items (B): " + maxItemsB)
		primitiveLogger.info("Max items (B): " + maxItemsB)

		val itemsIncB = cmd.getOptionValue("itemsIncB").toInt
		opLogger.info("Items inc (B): " + itemsIncB)
		primitiveLogger.info("Items inc (B): " + itemsIncB)
				
		// Data size:  A
		val minCharsA = cmd.getOptionValue("minCharsA").toInt
		opLogger.info("Min chars (A): " + minCharsA)
		primitiveLogger.info("Min chars (A): " + minCharsA)
		
		val maxCharsA = cmd.getOptionValue("maxCharsA").toInt
		opLogger.info("Max chars (A): " + maxCharsA)
		primitiveLogger.info("Max chars (A): " + maxCharsA)

		val charsIncA = cmd.getOptionValue("charsIncA").toInt
		opLogger.info("Chars inc (A): " + charsIncA)
		primitiveLogger.info("Chars inc (A): " + charsIncA)

		// Data size:  B
		val minCharsB = cmd.getOptionValue("minCharsB").toInt
		opLogger.info("Min chars (B): " + minCharsB)
		primitiveLogger.info("Min chars (B): " + minCharsB)

		val maxCharsB = cmd.getOptionValue("maxCharsB").toInt
		opLogger.info("Max chars (B): " + maxCharsB)
		primitiveLogger.info("Max chars (B): " + maxCharsB)

		val charsIncB = cmd.getOptionValue("charsIncB").toInt
		opLogger.info("Chars inc (B): " + charsIncB)
		primitiveLogger.info("Chars inc (B): " + charsIncB)


		// Setup
		val zookeeperServerAndPort = cmd.getOptionValue("zookeeperServerAndPort")
		//println("Zookeeper: " + zookeeperServerAndPort)
		opLogger.info("Zookeeper: " + zookeeperServerAndPort)
		primitiveLogger.info("Zookeeper: " + zookeeperServerAndPort)
		
		val storageNodeServer = cmd.getOptionValue("storageNodeServer")
		//println("Storage Node: " + storageNodeServer)
		opLogger.info("Storage Node: " + storageNodeServer)
		primitiveLogger.info("Storage Node: " + storageNodeServer)
	
		
		
		// Deploy cluster
		opLogger.info("Setting up full cluster...")
		primitiveLogger.info("Setting up full cluster...")

		implicit val env = new Environment
		env.session = new TrivialSession
		env.executor = new TrivialExecutor

		val n = new StorageNode(storageNodeServer, 9000)
		Queries.configureStorageEngine(n)  // set responsibility policy
		env.placement = new ZooKeptCluster(zookeeperServerAndPort)
		
		
		// Benchmark requested op
		var itemsRangeA:Seq[Int] = null
		var itemsRangeB:Seq[Int] = null
		var charsRangeA:Seq[Int] = null
		var charsRangeB:Seq[Int] = null
		
		if (oneBin) {
			itemsRangeA = List(minItemsA)
			itemsRangeB = List(minItemsB)

			charsRangeA = List(minCharsA)
			charsRangeB = List(minCharsB)
		} else {
			itemsRangeA = minItemsA to maxItemsA by itemsIncA
			itemsRangeB = minItemsB to maxItemsB by itemsIncB

			charsRangeA = minCharsA to maxCharsA by charsIncA
			charsRangeB = minCharsB to maxCharsB by charsIncB
		}
		
		
		var warmupDone = false
		
		charsRangeA.foreach((aSize) => {
			charsRangeB.foreach((bSize) => {
				//println("New data size pair:  size(A) = " + aSize + ", size(B) = " + bSize)
				opLogger.info("New data size pair:  size(A) = " + aSize + ", size(B) = " + bSize)
				primitiveLogger.info("New data size pair:  size(A) = " + aSize + ", size(B) = " + bSize)
				
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
					//println("ent_A is empty.")
					opLogger.info("ent_A is empty.")
					primitiveLogger.info("ent_A is empty.")
				}
				if (szB == 0) {
					//println("ent_B is empty.")
					opLogger.info("ent_B is empty.")
					primitiveLogger.info("ent_B is empty.")
				}

				
				// Generate data for requested op
				PerOpDataGen.genData(whichOp, aSize, bSize, maxItemsA, maxItemsB)
				
				// For each data size, get a bunch of measurements (varying the #s of A & B)
				itemsRangeA.foreach((numA) => {
					itemsRangeB.foreach((numB) => {

						// Warmup (only the current op, and only the first time through -- just need to hit all the code paths)
						if (warmupDuration > 0 & !warmupDone) {
							//println("Warming up jvm...")
							opLogger.info("Warming up jvm...")
							primitiveLogger.info("Warming up jvm...")
							val threads = (1 to numThreads).toList.map((id) => {	
								val agent = new BenchmarkAgent(whichOp, s_to_ms(warmupDuration), aSize, bSize, numA, numB, maxItemsA, maxItemsB)
								new Thread(agent)
							})

							for(thread <- threads) thread.start
							for(thread <- threads) thread.join

							warmupDone = true
						} else {
							//println("Skipping warmup...")
							opLogger.info("Skipping warmup...")
							primitiveLogger.info("Skipping warmup...")							
						}
						

						// Run
						//println("Starting run with #A=" + numA + ", #B=" + numB + "...")
						opLogger.info("Starting run with #A=" + numA + ", #B=" + numB + "...")
						primitiveLogger.info("Starting run with #A=" + numA + ", #B=" + numB + "...")
						val threads = (1 to numThreads).toList.map((id) => {	
							val agent = new BenchmarkAgent(whichOp, s_to_ms(runDuration), aSize, bSize, numA, numB, maxItemsA, maxItemsB)
							new Thread(agent)
						})

						// Run the test
						for(thread <- threads) thread.start
						for(thread <- threads) thread.join
					})
				})
			})
		})
		
		//println("Done with experiment.")
		opLogger.info("Done with experiment.")
		primitiveLogger.info("Done with experiment.")
		
		System.exit(0)
		
	}

	// Convert seconds to ms
	def s_to_ms(s_val:Int):Int = {
		s_val * 1000
	}

}


class BenchmarkAgent(opType:Int, duration_ms:Int, aSize:Int, bSize:Int, numA:Int, numB:Int, maxItemsA:Int, 
	maxItemsB:Int)(implicit env:Environment) extends Runnable {
	def run = {
		val opLogger = Logger.getLogger("scads.queryexecution.operators")
		val primitiveLogger = Logger.getLogger("scads.readpolicy.primitives")

		val startTime_ms = System.currentTimeMillis()
		val thinkTime_ms = 1
		
		var currentTime_ms = System.currentTimeMillis()
		while (currentTime_ms < (startTime_ms + duration_ms.longValue())) {
			PerOpBenchmarker.benchmarkOp(opType, aSize, bSize, numA, numB, maxItemsA, maxItemsB)
			
			//println(Thread.currentThread().getName() + " is thinking for " + thinkTime_ms + " ms")
			opLogger.info(Thread.currentThread().getName() + " is thinking for " + thinkTime_ms + " ms")
			primitiveLogger.info(Thread.currentThread().getName() + " is thinking for " + thinkTime_ms + " ms")
			Thread.sleep(thinkTime_ms)
			
			currentTime_ms = System.currentTimeMillis()
		}
		
	}
}


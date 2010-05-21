import deploylib._
import deploylib.ec2._
import deploylib.ParallelConversions._

import deploylib.runit._
import deploylib.config._
import deploylib.xresults._


// Properties shared by all runs
System.setProperty("numThreads", 5.toString)
System.setProperty("warmupDuration", 10.toString)
System.setProperty("runDuration", 10.toString)

// # data items:  A
System.setProperty("minItemsA", 5.toString)
System.setProperty("maxItemsA", 10.toString)
System.setProperty("itemsIncA", 5.toString)

// # data items:  B
System.setProperty("minItemsB", 5.toString)
System.setProperty("maxItemsB", 10.toString)
System.setProperty("itemsIncB", 5.toString)

// data size:  A
System.setProperty("minCharsA", 5.toString)
System.setProperty("maxCharsA", 10.toString)
System.setProperty("charsIncA", 5.toString)

// data size:  B
System.setProperty("minCharsB", 5.toString)
System.setProperty("maxCharsB", 10.toString)
System.setProperty("charsIncB", 5.toString)

// setup
System.setProperty("zookeeperPort", 2181.toString)
System.setProperty("storageEnginePort", 9000.toString)

// Per-op runs

//pforeach?  only if I change to args rather than system properties
//(1 to 9).foreach((i) => {
val i = 1
	println("Benchmarking op " + i + "...")

	// Deploy cluster for this run
	ClusterDeployment
//})


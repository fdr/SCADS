import deploylib._
import deploylib.ec2._
import deploylib.ParallelConversions._
import deploylib.runit._
import deploylib.config._
import deploylib.xresults._

import com.amazonaws.ec2._

import java.io._

// Experiment-type specific
var numExperiments = 0
var numThreads = 0
var whichOp = 0
var varyOpNum = true
var varyThreadNum = true

// Pretty generic
val oneBin = true
val runDuration = 60
val warmupDuration = 300
val min = 10
val threadsRange = 5 to 50 by 5
//val ops = 1 to 9 by 1
val ops = List(2, 5, 6, 8)
val bucket = "kristal/run-" + System.currentTimeMillis()

//val experimentType = "benchmarkAllOps"
//val experimentType = "vary#threads"
val experimentType = "benchmarkOneOpMultipleTimes"

experimentType match {
	case "benchmarkAllOps" => 
		varyOpNum = true
		varyThreadNum = false
		numExperiments = ops.length
		numThreads = 5
	case "vary#threads" =>
		varyOpNum = false
		varyThreadNum = true
		numExperiments = threadsRange.length
		whichOp = 3
	case "benchmarkOneOpMultipleTimes" =>
		varyOpNum = false
		varyThreadNum = false
		numThreads = 5
		whichOp = 5
		numExperiments = 10
}

val experiments = 1 to numExperiments by 1
println("Acquiring instances...")
val nodes = EC2Instance.runInstances("ami-e7a2448e", numExperiments, numExperiments, System.getenv("AWS_KEY_NAME"), "m1.small", "us-east-1b")
var clientArgs = new Array[String](numExperiments)

experiments.pforeach((i) => {
	nodes(i-1).upload(new File(System.getenv("AWS_KEY_PATH")), new File("/root"))

	// Set AWS params
	nodes(i-1).createFile(new File("/root/vars.txt"), Array(
		"export AWS_KEY_NAME=" + System.getenv("AWS_KEY_NAME"),
		"export AWS_SECRET_ACCESS_KEY=UXZ7FDk74XQk4NxgN9K0oK6iot7eL1A1V/6xX2LB",
		"export AWS_ACCESS_KEY_ID=026FE3D736A8XTV3D382",
		"export AWS_KEY_PATH=/root/" + System.getenv("AWS_KEY_NAME")).mkString("", "\n", "\n")
	)
	nodes(i-1).executeCommand("cat /root/.bash_profile /root/vars.txt > /root/.bash_profile")
	nodes(i-1).executeCommand("source /root/.bash_profile")

	// Get jars from S3
	nodes(i-1).executeCommand("s3cmd get s3://kristal/scalaengine-1.1-SNAPSHOT-jar-with-dependencies.jar")
	nodes(i-1).executeCommand("s3cmd get s3://kristal/benchmarker-1.0-SNAPSHOT-jar-with-dependencies.jar")
	
	if (varyThreadNum && !varyOpNum) {
		clientArgs(i-1) = ("screen java -Dbucket=" + bucket + " -DfilenamePrefix=op" + whichOp + "-threads" + threadsRange(i-1) + "-take" + (i-1)
			+ " -DwhichOp=" + whichOp + " -DnumThreads=" + threadsRange(i-1) + " -DwarmupDuration=" + warmupDuration 
			+ " -DrunDuration=" + runDuration + " -DoneBin=" + oneBin
			+ " -DminItemsA=" + min
			+ " -DminItemsB=" + min
			+ " -DminCharsA=" + min
			+ " -DminCharsB=" + min
			+ " -DzookeeperPort=2181 -DstorageEnginePort=9000 -DavailabilityZone=us-east-1b -DremoteDir=/root -cp benchmarker-1.0-SNAPSHOT-jar-with-dependencies.jar ClusterDeployment\n")
	}
	
	if (!varyThreadNum && varyOpNum) {
		clientArgs(i-1) = ("screen java -Dbucket=" + bucket + " -DfilenamePrefix=op" + ops(i-1) + "-threads" + numThreads + "-take" + (i-1)
			+ " -DwhichOp=" + ops(i-1) + " -DnumThreads=" + numThreads + " -DwarmupDuration=" + warmupDuration 
			+ " -DrunDuration=" + runDuration + " -DoneBin=" + oneBin
			+ " -DminItemsA=" + min
			+ " -DminItemsB=" + min
			+ " -DminCharsA=" + min
			+ " -DminCharsB=" + min
			+ " -DzookeeperPort=2181 -DstorageEnginePort=9000 -DavailabilityZone=us-east-1b -DremoteDir=/root -cp benchmarker-1.0-SNAPSHOT-jar-with-dependencies.jar ClusterDeployment\n")
	}

	if (!varyThreadNum && !varyOpNum) {
		clientArgs(i-1) = ("screen java -Dbucket=" + bucket + " -DfilenamePrefix=op" + whichOp + "-threads" + numThreads + "-take" + (i-1)
			+ " -DwhichOp=" + whichOp + " -DnumThreads=" + numThreads + " -DwarmupDuration=" + warmupDuration 
			+ " -DrunDuration=" + runDuration + " -DoneBin=" + oneBin
			+ " -DminItemsA=" + min
			+ " -DminItemsB=" + min
			+ " -DminCharsA=" + min
			+ " -DminCharsB=" + min
			+ " -DzookeeperPort=2181 -DstorageEnginePort=9000 -DavailabilityZone=us-east-1b -DremoteDir=/root -cp benchmarker-1.0-SNAPSHOT-jar-with-dependencies.jar ClusterDeployment\n")
	}
	
	nodes(i-1).createFile(new File("/root/run.txt"), clientArgs(i-1))

	println("ssh-not-strict root@" + nodes(i-1).hostname)	

})


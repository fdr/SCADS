import deploylib._
import deploylib.ec2._
import deploylib.ParallelConversions._
import deploylib.runit._
import deploylib.config._
import deploylib.xresults._

import com.amazonaws.ec2._

import java.io._

// Set params:
// Specific to experiment type
var numExperiments = 0
var mixChoice:String = null
var varyQueryType = true

// Pretty generic
var numThreads = 5
val runDuration = 60
val warmupDuration = 300
val mixChoices = List("userByName", "userByEmail", "thoughtstream", "thoughtsByHashTag")

val experimentType = "benchmarkAllQueries"
//val experimentType = "benchmarkOneQueryMultipleTimes"

experimentType match {
	case "benchmarkAllQueries" => 
		varyQueryType = true
		numExperiments = mixChoices.length
	case "benchmarkOneQueryMultipleTimes" =>
		varyQueryType = false
		numExperiments = 10
		mixChoice = "userByEmail"
}

val experiments = 0 to (numExperiments-1) by 1
println("Acquiring instances...")
val nodes = EC2Instance.runInstances("ami-e7a2448e", numExperiments, numExperiments, System.getenv("AWS_KEY_NAME"), "m1.small", "us-east-1b")
var runCmd = new Array[String](numExperiments)

experiments.pforeach((i) => {
	nodes(i).upload(new File(System.getenv("AWS_KEY_PATH")), new File("/root"))

	// Set AWS params
	nodes(i).createFile(new File("/root/vars.txt"), Array(
		"export AWS_KEY_NAME=" + System.getenv("AWS_KEY_NAME"),
		"export AWS_SECRET_ACCESS_KEY=UXZ7FDk74XQk4NxgN9K0oK6iot7eL1A1V/6xX2LB",
		"export AWS_ACCESS_KEY_ID=026FE3D736A8XTV3D382",
		"export AWS_KEY_PATH=/root/" + System.getenv("AWS_KEY_NAME")).mkString("", "\n", "\n")
	)
	nodes(i).executeCommand("cat /root/.bash_profile /root/vars.txt > /root/.bash_profile")
	nodes(i).executeCommand("source /root/.bash_profile")

	// Get jars from S3
	nodes(i).executeCommand("s3cmd get s3://kristal/scalaengine-1.1-SNAPSHOT-jar-with-dependencies.jar")
	nodes(i).executeCommand("s3cmd get s3://kristal/querygen-1.0-SNAPSHOT-jar-with-dependencies.jar")
	
	if (varyQueryType) {
		runCmd(i) = ("java -cp querygen-1.0-SNAPSHOT-jar-with-dependencies.jar DeployQuerygen -availabilityZone=us-east-1b -zookeeperPort=2181 -storageEnginePort=9000"
			+ " -numUsers=1000 -thoughtsPerUser=20 -subsPerUser=10 -emailDupFactor=10 -thoughtstreamLength=10 -numHashTagsPerThought=5 -numDistinctHashTags=10"
			+ " -numThreads=" + numThreads + " -mixChoice=" + mixChoices(i) + " -duration=60 -warmupDuration=300\n")
	} else {
		runCmd(i) = ("java -cp querygen-1.0-SNAPSHOT-jar-with-dependencies.jar DeployQuerygen -availabilityZone=us-east-1b -zookeeperPort=2181 -storageEnginePort=9000"
			+ " -numUsers=1000 -thoughtsPerUser=20 -subsPerUser=10 -emailDupFactor=10 -thoughtstreamLength=10 -numHashTagsPerThought=5 -numDistinctHashTags=10"
			+ " -numThreads=" + numThreads + " -mixChoice=" + mixChoice + " -duration=60 -warmupDuration=300\n")
	}
	
	nodes(i).createFile(new File("/root/run.txt"), runCmd(i))
	nodes(i).executeCommand("`cat /root/run.txt`")

	println("ssh-not-strict root@" + nodes(i).hostname)	
	
})


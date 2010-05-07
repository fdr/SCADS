import deploylib._
import deploylib.ec2._
import deploylib.ParallelConversions._

import deploylib.runit._
import deploylib.config._
import deploylib.xresults._

import com.amazonaws.ec2._

import java.io._

val whichOp=3
val remoteDir = "/root"

/*
// Set experiment params
System.setProperty("whichOp", 1.toString)
System.setProperty("numThreads", 50.toString)
System.setProperty("warmupDuration", 300.toString)
System.setProperty("runDuration", 60.toString)

// # data items:  A
System.setProperty("minItemsA", 10.toString)
System.setProperty("maxItemsA", 100.toString)
System.setProperty("itemsIncA", 30.toString)

// # data items:  B
System.setProperty("minItemsB", 10.toString)
System.setProperty("maxItemsB", 100.toString)
System.setProperty("itemsIncB", 30.toString)

// data size:  A
System.setProperty("minCharsA", 10.toString)
System.setProperty("maxCharsA", 100.toString)
System.setProperty("charsIncA", 30.toString)

// data size:  B
System.setProperty("minCharsB", 10.toString)
System.setProperty("maxCharsB", 100.toString)
System.setProperty("charsIncB", 30.toString)

// setup
System.setProperty("zookeeperPort", 2181.toString)
System.setProperty("storageEnginePort", 9000.toString)
System.setProperty("availabilityZone", "us-east-1b")

val remoteDir = "/root"
System.setProperty("remoteDir", remoteDir)
//val instanceID = "i-07acef6c"
*/

// Get instance
//val instance = EC2Instance.getInstance(instanceID)
var instance:EC2Instance = null
try {
	instance = EC2Instance.runInstance
	instance
} catch {
	case e:AmazonEC2Exception => 
		instance = EC2Instance.runInstances("ami-e7a2448e", 1, 1, System.getenv("AWS_KEY_NAME"), "m1.small", "us-east-1b")(0)
		instance
}

// Upload my keypair file
instance.upload(new File("/Users/ksauer/ksauer-keypair"), new File(remoteDir))

// Set AWS params
instance.createFile(new File("/root/vars.txt"), Array(
	"export AWS_KEY_NAME=/home/eecs/ksauer/.ec2/ksauer-keypair",
	"export AWS_SECRET_ACCESS_KEY=UXZ7FDk74XQk4NxgN9K0oK6iot7eL1A1V/6xX2LB",
	"export AWS_ACCESS_KEY_ID=026FE3D736A8XTV3D382",
	"export AWS_KEY_PATH=" + remoteDir + "/ksauer-keypair").mkString("", "\n", "\n")
)
instance.executeCommand("cat /root/.bash_profile /root/vars.txt > /root/.bash_profile")

instance.executeCommand("export AWS_KEY_NAME=/home/eecs/ksauer/.ec2/ksauer-keypair")
instance.executeCommand("export AWS_SECRET_ACCESS_KEY=UXZ7FDk74XQk4NxgN9K0oK6iot7eL1A1V/6xX2LB")
instance.executeCommand("export AWS_ACCESS_KEY_ID=026FE3D736A8XTV3D382")
instance.executeCommand("export AWS_KEY_PATH=" + remoteDir + "/ksauer-keypair")

/*
// Upload jars (scalaengine, benchmarking)
instance.upload(new File("/Users/ksauer/Desktop/scads/scalaengine/target/scalaengine-1.1-SNAPSHOT-jar-with-dependencies.jar"), new File(remoteDir))
instance.upload(new File("/Users/ksauer/Desktop/scads/experiments/client/performance/benchmarking/target/benchmarker-1.0-SNAPSHOT-jar-with-dependencies.jar"),
	new File(remoteDir))
*/

// Get jars from S3
instance.executeCommand("s3cmd get s3://kristal/scalaengine-1.1-SNAPSHOT-jar-with-dependencies.jar")
instance.executeCommand("s3cmd get s3://kristal/benchmarker-1.0-SNAPSHOT-jar-with-dependencies.jar")

// Run benchmark
/*
instance.executeCommand("java -DwhichOp=1 -DnumThreads=50 -DwarmupDuration=300 -DrunDuration=60 -DminItemsA=10 "
	+ "-DmaxItemsA=100 -DitemsIncA=30 -DminItemsB=10 -DmaxItemsB=100 -DitemsIncB=30 -DminCharsA=10 -DmaxCharsA=100 "
	+ "-DcharsIncA=30 -DminCharsB=10 -DmaxCharsB=100 -DcharsIncB=30 -DzookeeperPort=2181 -DstorageEnginePort=9000 "
	+ "-DremoteDir=/root -DavailabilityZone=\"us-east-1b\" -cp benchmarker-1.0-SNAPSHOT-jar-with-dependencies.jar ClusterDeployment")
*/
//ClusterDeployment

instance.createFile(new File("/root/run.txt"), "screen java -DwhichOp="+whichOp+" -DnumThreads=50 -DwarmupDuration=300 -DrunDuration=60 -DminItemsA=10 -DmaxItemsA=100 -DitemsIncA=30 -DminItemsB=10 -DmaxItemsB=100 -DitemsIncB=30 -DminCharsA=10 -DmaxCharsA=100 -DcharsIncA=30 -DminCharsB=10 -DmaxCharsB=100 -DcharsIncB=30 -DzookeeperPort=2181 -DstorageEnginePort=9000 -DavailabilityZone=us-east-1b -DremoteDir=/root -cp benchmarker-1.0-SNAPSHOT-jar-with-dependencies.jar ClusterDeployment\n")
println(instance.hostname)
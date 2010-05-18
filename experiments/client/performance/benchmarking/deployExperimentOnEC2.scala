import deploylib._
import deploylib.ec2._
import deploylib.ParallelConversions._
import deploylib.runit._
import deploylib.config._
import deploylib.xresults._

import com.amazonaws.ec2._

import java.io._

val whichOp=6
val remoteDir = "/root"
val numThreads = 5
val min=10
//val max=20
//val inc=10
val oneBin=true

// Get instance
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
instance.upload(new File(System.getenv("AWS_KEY_PATH")), new File(remoteDir))

// Set AWS params
instance.createFile(new File("/root/vars.txt"), Array(
	"export AWS_KEY_NAME=" + System.getenv("AWS_KEY_NAME"),
	"export AWS_SECRET_ACCESS_KEY=UXZ7FDk74XQk4NxgN9K0oK6iot7eL1A1V/6xX2LB",
	"export AWS_ACCESS_KEY_ID=026FE3D736A8XTV3D382",
	"export AWS_KEY_PATH=" + remoteDir + "/" + System.getenv("AWS_KEY_NAME")).mkString("", "\n", "\n")
)
instance.executeCommand("cat /root/.bash_profile /root/vars.txt > /root/.bash_profile")
instance.executeCommand("source /root/.bash_profile")

// Get jars from S3
instance.executeCommand("s3cmd get s3://kristal/scalaengine-1.1-SNAPSHOT-jar-with-dependencies.jar")
instance.executeCommand("s3cmd get s3://kristal/benchmarker-1.0-SNAPSHOT-jar-with-dependencies.jar")

// Run benchmark
/*
instance.createFile(new File("/root/run.txt"), "screen java -DwhichOp="+whichOp+" -DnumThreads="+numThreads+" -DwarmupDuration=300 -DrunDuration=60"
+ " -DminItemsA=" + min + " -DmaxItemsA=" + max + " -DitemsIncA=" + inc + " -DminItemsB=" + min + " -DmaxItemsB=" + max + " -DitemsIncB=" + inc 
+ " -DminCharsA=" + min + " -DmaxCharsA=" + max + " -DcharsIncA=" + inc + " -DminCharsB=" + min + " -DmaxCharsB=" + max + " -DcharsIncB=" + inc
+ " -DzookeeperPort=2181 -DstorageEnginePort=9000 -DavailabilityZone=us-east-1b -DremoteDir=/root -cp benchmarker-1.0-SNAPSHOT-jar-with-dependencies.jar ClusterDeployment\n")
*/
instance.createFile(new File("/root/run.txt"), "screen java -DwhichOp="+whichOp+" -DnumThreads="+numThreads+" -DwarmupDuration=300 -DrunDuration=60"
+ " -DminItemsA=" + min 
+ " -DminItemsB=" + min
+ " -DminCharsA=" + min
+ " -DminCharsB=" + min
+ " -DoneBin=true"
+ " -DzookeeperPort=2181 -DstorageEnginePort=9000 -DavailabilityZone=us-east-1b -DremoteDir=/root -cp benchmarker-1.0-SNAPSHOT-jar-with-dependencies.jar ClusterDeployment\n")

println(instance.hostname)
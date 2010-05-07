import deploylib._
import deploylib.ec2._
import deploylib.ParallelConversions._

import deploylib.runit._
import deploylib.config._
import deploylib.xresults._

import com.amazonaws.ec2._

import java.io._

val whichOp=9
val instanceID="i-27fbb94c"

EC2Instance.update
val node = EC2Instance.getInstance(instanceID)

node.executeCommand("s3cmd get s3://kristal/logparsing-1.0-SNAPSHOT-jar-with-dependencies.jar")
node.createFile(new File("/root/parse.txt"), "java -DlogDir=/root/operator -DoutputFilename=/root/op" + whichOp + ".csv -cp logparsing-1.0-SNAPSHOT-jar-with-dependencies.jar parser.ParseOperatorLogs\n")

println(node.hostname)
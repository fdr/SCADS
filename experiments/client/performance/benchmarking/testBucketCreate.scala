import deploylib._
import deploylib.ec2._
import deploylib.ParallelConversions._

import deploylib.runit._
import deploylib.config._
import deploylib.xresults._

import com.amazonaws.ec2._

import java.io._

val instanceID="i-774a6f1c"

EC2Instance.update
val node = EC2Instance.getInstance(instanceID)

node.executeCommand("s3cmd mb s3://kristal/test-" + System.currentTimeMillis())
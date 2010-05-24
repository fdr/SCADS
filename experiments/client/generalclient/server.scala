import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.storage._

val path = new java.io.File("testdb")
path.mkdir()
val zooKeeper = ZooKeep.start("target/testCluster", 2111).root.getOrCreate("scads")
val handler = ScalaEngine.main(9000, "localhost:2111", Some(path), None, false,false)
val node = RemoteNode("localhost", 9000)
val cluster = new ScadsCluster(zooKeeper)

System.in.read()


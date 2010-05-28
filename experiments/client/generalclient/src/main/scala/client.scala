package client

import edu.berkeley.cs.scads.piql._
import edu.berkeley.cs.scads.storage._


object PiqlTester extends Application {
	//val zoo = new edu.berkeley.cs.scads.comm.ZooKeeperProxy("localhost:2111")
	//val cluster = new edu.berkeley.cs.scads.storage.ScadsCluster(zoo.root.getOrCreate("scads"))

	implicit val env = piql.Configurator.configure(TestScalaEngine.cluster)

	/*
	// figuring out how to delete a namespace
	val ns = TestScalaEngine.cluster.namespaces
	println(ns)
	ns.deleteChild("ent_A")
	println(ns)
	*/

	//val a = new piql.A
	val a = new piql.A
	a.key1 = "test"
	a.key2 = "test2"
	a.save

	println(piql.Queries.AByKey("test"))
	
	System.exit(0)
}

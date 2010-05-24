//val zoo = new edu.berkeley.cs.scads.comm.ZooKeeperProxy("localhost:2111")
//val cluster = new edu.berkeley.cs.scads.storage.ScadsCluster(zoo.root.getOrCreate("scads"))

implicit val env = piql.Configurator.configure(edu.berkeley.cs.scads.storage.TestScalaEngine.cluster)

val a = new piql.A
a.key1 = "test"
a.key2 = "test2"
a.save

println(piql.Queries.AByKey("test"))

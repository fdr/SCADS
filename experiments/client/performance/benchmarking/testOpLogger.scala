import org.apache.log4j._
import edu.berkeley.cs.scads.model._

val logger = Logger.getLogger("scads.queryexecution.operators")

val filename = "/Users/ksauer/Desktop/log4jConfig-scads.txt"
PropertyConfigurator.configure(filename)
		
implicit val env = new Environment
env.placement = new TestCluster
env.session = new TrivialSession
env.executor = new TrivialExecutor
		
val a = new A
a.key("key")
a.key2("key2")
a.dataA("dataA")
a.save

println((new A).singleGet("ent_A", StringField.apply("key"), ReadRandomPolicy))

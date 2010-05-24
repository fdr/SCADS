package ops

import edu.berkeley.cs.scads.piql._
import edu.berkeley.cs.scads.piql.parser._
import edu.berkeley.cs.scads.storage._

import org.apache.log4j._
import org.apache.log4j.Level._
import scala.collection.mutable._


object CallOps extends Application with QueryExecutor {
	BasicConfigurator.configure()
	
	implicit val env = piql.Configurator.configure(TestScalaEngine.cluster)		// Why do I have to do ".piql"?

	val a = new piql.A
	a.key1 = "test"
	a.key2 = "test2"
	a.save
	
	println(singleGet("ent_A", List(BoundStringValue("test"))))
	
	System.exit(0)
}


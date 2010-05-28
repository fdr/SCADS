import edu.berkeley.cs.scads.model.Environment
import edu.berkeley.cs.scads.model.{TrivialExecutor,TrivialSession}
import edu.berkeley.cs.scads.TestCluster
import edu.berkeley.cs.scads.TestClusterWithDb
import org.apache.log4j._
import org.apache.log4j.Level._


implicit val env = new Environment
env.placement = new TestClusterWithDb("target/db9000")
env.session = new TrivialSession
env.executor = new TrivialExecutor


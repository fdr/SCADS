import edu.berkeley.cs.scads.model._
import org.apache.log4j._
import org.apache.log4j.Level._
import edu.berkeley.cs.scads.thrift._
import scala.collection.mutable._


implicit val env = new Environment
env.placement = new TestCluster
env.session = new TrivialSession
env.executor = new TrivialExecutor


val opType=9
val maxItems=100
val aSize=10
val bSize=10
val numA=20
val numB=20

PerOpDataGen.genData(opType, aSize, bSize, maxItems)

PerOpBenchmarker.benchmarkOp(opType, aSize, bSize, numA, numB, maxItems)
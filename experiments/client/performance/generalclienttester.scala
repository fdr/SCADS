import edu.berkeley.cs.scads.model._
import org.apache.log4j._
import org.apache.log4j.Level._
import edu.berkeley.cs.scads.thrift._
import scala.collection.mutable._


implicit val env = new Environment
env.placement = new TestCluster
env.session = new TrivialSession
env.executor = new TrivialExecutor


(1 to 10).foreach((j) => {

	if (j != 6) {
		val a = new A
		a.key("key-" + j)
		a.dataA("dataA-" + j)
		a.save

		(1 to 10).foreach((i) => {
			val b = new B
			b.key2("keyb-" + i)
			b.dataB("dataB-" + i)
			b.owner(a)
			b.save
		})
	}
})


// Why not use EC2 for this??
// Can spin up one small cluster per op (one for storage node, one for zookeeper, one for op generator)
// What to pass in as storage node server?  Usually, it's "r13" or something like that.
// What ports to use on EC2 nodes?
// Ask Michael

//def singleGet(namespace: String, key: Field, policy: ReadPolicy)(implicit env: Environment): TupleStream = {	
println("singleGet")
println((new A).singleGet("ent_A", StringField.apply("key-1"), ReadRandomPolicy))
println("")


//def prefixGet(namespace: String, prefix: Field, limit: LimitValue, ascending: Boolean, policy: ReadPolicy)(implicit env: Environment): TupleStream = {
println("prefixGet")
println((new A).prefixGet("ent_B", StringField.apply("key-1"), IntegerField.apply(5), true, ReadRandomPolicy))
println("")


//def sequentialDereferenceIndex(targetNamespace: String, policy: ReadPolicy, child: TupleStream)(implicit env: Environment): TupleStream = {
val a1 = new A
a1.key("key-11")
a1.key2("SAME")
a1.dataA("dataA-6")
a1.save

val a2 = new A
a2.key("key-6")
a2.key2("SAME")
a2.dataA("dataA-6")
a2.save

var idxTupleStream = (new Tuple(a1.key, new IntegerVersion, a1.serializeAttributes)) :: (new Tuple(a2.key, new IntegerVersion, a2.serializeAttributes)) :: Nil
//println(idxTupleStream)

println("sequentialDereferenceIndex:")
/*
val rtnIdxTupleStream = (new A).sequentialDereferenceIndex("idxAdataA", ReadRandomPolicy, idxTupleStream)
println(rtnIdxTupleStream)
println("returned " + rtnIdxTupleStream.length + " tuples")
*/
//println((new A).prefixGet("idxAdataA", StringField.apply("dataA-6"), IntegerField.apply(5), true, ReadRandomPolicy))
println((new A).prefixGet("idxAkey2", StringField.apply("SAME"), IntegerField.apply(5), true, ReadRandomPolicy))
//val sDITuples = (new A).prefixGet("idxAdataA", StringField.apply("dataA-6"), IntegerField.apply(5), true, ReadRandomPolicy)
val sDITuples = (new A).prefixGet("idxAkey2", StringField.apply("SAME"), IntegerField.apply(5), true, ReadRandomPolicy)
val rtnSDITuples = (new A).sequentialDereferenceIndex("ent_A", ReadRandomPolicy, sDITuples)
println("input: " + sDITuples)
println("output: " + rtnSDITuples)
println("")

// in future, use "key2" instead of "dataA" as the secondary key, since the dataA field will vary a lot in length and will be full of garbage


//def prefixJoin(namespace: String, conditions: List[JoinCondition], limit: LimitValue, ascending: Boolean, policy: ReadPolicy, child: EntityStream)(implicit env: Environment): TupleStream = {
var aStream:List[A] = Nil

(1 to 10).foreach((i) => {
	val a = new A
	a.key("key-" + i)
	a.dataA("dataA-" + i)
	//a.save

	aStream = a :: aStream
})
aStream = aStream.reverse


println("prefixJoin")
// "get me the B's that belong to this A"
//println((new A).prefixGet("ent_B", StringField.apply("key-1"), IntegerField.apply(5), true, ReadRandomPolicy))

val tuples = (new A).prefixJoin("ent_B", List(AttributeCondition("key")), IntegerField.apply(5), true, ReadRandomPolicy, aStream)
println("#tuples = " + tuples.length)
println("")


//def pointerJoin(namespace: String, conditions: List[JoinCondition], policy: ReadPolicy, child: EntityStream)(implicit env: Environment): TupleStream = {
var bStream:List[B] = Nil

(1 to 10).foreach((i) => {
	val a = new A
	a.key("key-" + i)
	a.dataA("dataA-" + i)
	
	val b = new B
	b.key2("keyb-" + i)
	b.dataB("dataB-" + i)
	b.owner(a)

	bStream = b :: bStream
})
bStream = bStream.reverse	
	
	
println("pointerJoin")
// get me the A to which this B belongs
val pointerTuples = (new A).pointerJoin("ent_A", List(AttributeCondition("owner")), ReadRandomPolicy, bStream)
println(pointerTuples)
println("#tuples = " + pointerTuples.length)
println("")


//def materialize[EntityType <: Entity](entityClass: Class[EntityType], child: TupleStream)(implicit env: Environment): Seq[EntityType] = {
var aTupleStream:List[Tuple] = Nil
(1 to 10).foreach((i) => {
	val a = new A
	a.key("key-" + i)
	a.dataA("dataA-" + i)
	
	aTupleStream = (new Tuple(a.key, new IntegerVersion, a.serializeAttributes)) :: aTupleStream
})
aTupleStream = aTupleStream.reverse

println("materialize:")
val aSeq = (new A).materialize(classOf[A], aTupleStream)
println(aSeq)
println("#A's = " + aSeq.length)
println("")


// def selection[EntityType <: Entity](equalityMap: HashMap[String, Field], child: Seq[EntityType]): Seq[EntityType] = {
var aEntitySeq:List[A] = Nil

(1 to 10).foreach((i) => {
	val a = new A
	a.key("key-" + i)
	a.dataA("dataA-" + i)

	aEntitySeq = a :: aEntitySeq
})
aEntitySeq = aEntitySeq.reverse

println("selection:")
// HashMap:  ("fieldName", what the value should be)
val aSelectionSeq = (new A).selection(HashMap(("key", StringField.apply("key-4"))).asInstanceOf[HashMap[String,Field]], aEntitySeq)
println(aSelectionSeq)
println("")
// How to return more than one here?  Table like subscription?


//def sort[EntityType <: Entity](fields: List[String], ascending: Boolean, child: Seq[EntityType]): Seq[EntityType] = {
println("sort:")
val sortSeq = aEntitySeq.reverse
val sortedSeq = (new A).sort(List("key"), true, sortSeq)
println("before: " + sortSeq)
println("after:" + sortedSeq)
println("")


//def topK[EntityType <: Entity](k: Field, child: Seq[EntityType]): Seq[EntityType] = {
println("topK:")
val top = (new A).topK(IntegerField(5), sortSeq)
println(top)
println("length(top) = " + top.length)








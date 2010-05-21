/*
import edu.berkeley.cs.scads.model.Environment
import edu.berkeley.cs.scads.model.{TrivialExecutor,TrivialSession}
import edu.berkeley.cs.scads.model.TestCluster
*/
import edu.berkeley.cs.scads.model._
import org.apache.log4j._
import org.apache.log4j.Level._
import edu.berkeley.cs.scads.thrift._
import querygen.RandomUser


implicit val env = new Environment
env.placement = new TestCluster
env.session = new TrivialSession
env.executor = new TrivialExecutor


// Setup params
/*
Data parameters:
  numUsers=>1000<
  thoughtsPerUser=>20<
  subsPerUser=>10<
  emailDupFactor=>10<
  thoughtstreamLength=>10<
  numHashTagsPerThought=>5<
  numDistinctHashTags=>10<
*/
val numUsers=50
val thoughtsPerUser=20
val subsPerUser=20
val emailDupFactor=10
val thoughtstreamLength=10
val numHashTagsPerThought=10
val numDistinctHashTags=10

val dataManager = new SCADrDataManager

val paramMap = dataManager.getParamMap(numUsers, emailDupFactor, thoughtstreamLength, numDistinctHashTags)
dataManager.populateDB(paramMap, emailDupFactor, thoughtsPerUser, subsPerUser, numHashTagsPerThought)

val user1 = new user
user1.name("user1")
user1.password("secret")
user1.email("user1@berkeley.edu")
println(user1.myFollowing(10))

/*
// Try a query
println("userByName query:")
println(Queries.userByName("user1").apply(0))
println("");
*/

// Try an op
//val policy = new ReadRandomPolicy		// wrong, b/c ReadRandomPolicy is an OBJECT, not a class
val namespace = "ent_user"
val key = "user1"
//println(QueryExecutor.singleGet(namespace, key, policy))

println("userByName query:")
val u = Queries.userByName("user1").apply(0)
println("");


println("singleGet:")
println(u.singleGet(u.namespace, u.name, ReadRandomPolicy))
println("");


println("prefixGet:")
//Mon Mar 08 10:27:44 PST 2010: Thread-51 executed: prefixGet(ent_hashTag,StringField<tag10>), start=2.906269091796927E9, end=2.906269144299173E9, latency=52.502246
//def prefixGet(namespace: String, prefix: Field, limit: LimitValue, ascending: Boolean, policy: ReadPolicy)(implicit env: Environment): TupleStream = {
println(u.prefixGet("ent_user", StringField.apply("user1"), IntegerField.apply(10), true, ReadRandomPolicy))
println("")


println("sequentialDereferenceIndex:")
//def sequentialDereferenceIndex(targetNamespace: String, policy: ReadPolicy, child: TupleStream)(implicit env: Environment): TupleStream = {
val namespace_sDI = "ent_user"
	
// make a TupleStream
//class Tuple(key: Field, version: Version, value: String)
val numTuples = 10
var tupleStream: List[Tuple] = Nil

val version = new IntegerVersion

(1 to numTuples).foreach((i) => {
	val u2 = new user
	u2.name("user1")
	u2.password("secret")
	u2.email("user2@berkeley.edu")
	//u2.save	// necessary?
	//tupleStream = (new Tuple(u2.name, version, u2.serializeAttributes)) :: tupleStream

	tupleStream = (new Tuple(u2.email, version, u2.name.serializeKey)) :: tupleStream	
})
tupleStream = tupleStream.reverse
println(tupleStream)

println(u.sequentialDereferenceIndex(namespace_sDI, ReadRandomPolicy, tupleStream))
println("")


println("prefixJoin:")
//def prefixJoin(namespace: String, conditions: List[JoinCondition], limit: LimitValue, ascending: Boolean, policy: ReadPolicy, child: EntityStream)(implicit env: Environment): TupleStream = {
val namespacePrefixJoin = "ent_subscription"
var entityStreamPrefixJoin: List[user] = Nil
val numTuplesPrefixJoin = 10

(1 to numTuplesPrefixJoin).foreach((i) => {
	val u3 = new user
	u3.name("user1")
	u3.password("secret")
	u3.email("user2@berkeley.edu")

	entityStreamPrefixJoin = u3 :: entityStreamPrefixJoin	
})
entityStreamPrefixJoin = entityStreamPrefixJoin.reverse
println(entityStreamPrefixJoin)

//..., "ent_subscription", List(AttributeCondition("name")), IntegerField(5000), true, ReadRandomPolicy, ...
//println(u.prefixJoin(namespacePrefixJoin, List(AttributeCondition("name")), IntegerField(5000), true, ReadRandomPolicy, entityStreamPrefixJoin))
u.prefixJoin(namespacePrefixJoin, List(AttributeCondition("name")), IntegerField(5000), true, ReadRandomPolicy, entityStreamPrefixJoin)
println("")

/*
println("pointerJoin:")
// "get the user who's the target of this subscription"
//def pointerJoin(namespace: String, conditions: List[JoinCondition], policy: ReadPolicy, child: EntityStream)(implicit env: Environment): TupleStream = {
//[INFO]          pointerJoin("ent_user", List(AttributeCondition("target")), ReadRandomPolicy, <subscriptions>
val namespacePointerJoin = "ent_user"
var entityStreamPointerJoin:List[subscription] = Nil
val numTuplesPointerJoin = 10

(1 to numTuplesPointerJoin).foreach((i) => {
	var sub = new subscription
	sub.approved.value = true
	sub.owner("user1")
	sub.target("user2")
	
	if (i == 1)
		sub.save

	entityStreamPointerJoin = sub :: entityStreamPointerJoin	
})
entityStreamPointerJoin = entityStreamPointerJoin.reverse
//println(entityStreamPointerJoin)

println(u.pointerJoin(namespacePointerJoin, List(AttributeCondition("target")), ReadRandomPolicy, entityStreamPointerJoin))
println("")
*/


println("materialize")
// "turns a Tuple into an Entity"

var materializeTupleStream:List[Tuple] = Nil
val numTuplesMaterialize = 10

(1 to numTuplesMaterialize).foreach((i) => {
	val u2 = new user
	u2.name("user1")
	u2.password("secret")
	u2.email("user2@berkeley.edu")

	materializeTupleStream = (new Tuple(u2.name, version, u2.serializeAttributes)) :: materializeTupleStream	
})
materializeTupleStream = materializeTupleStream.reverse
println(materializeTupleStream)

println(u.materialize(classOf[user], materializeTupleStream))
println("")


println("subscription")
// "i'll give you all my subscriptions; give me back only those subscriptions that have been approved"
// [INFO]           selection(HashMap(("approved", TrueField)).asInstanceOf[HashMap[String, Field]], <list of the user's subscriptions>)








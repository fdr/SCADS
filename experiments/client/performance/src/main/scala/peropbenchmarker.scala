import edu.berkeley.cs.scads.model._
import org.apache.log4j._
import org.apache.log4j.Level._
import edu.berkeley.cs.scads.thrift._

object PerOpBenchmarker {
	def benchmarkOp(opType:Int, aSize:Int, numA:Int, numB:Int, maxItems:Int)(implicit env: Environment) = {
		opType match {
			case 1 => benchmarkOp1(maxItems)
			case 2 => benchmarkOp2(numB, maxItems)
			case 3 => benchmarkOp3(numA, maxItems)
			case 4 => benchmarkOp4(aSize, numA, numB)
			/*
			case 5 => benchmarkOp5
			case 6 => benchmarkOp6
			case 7 => benchmarkOp7
			case 8 => benchmarkOp8
			case 9 => benchmarkOp9
			*/ 
		}
		
	}
	
	
	// Util functions
	protected def getRandomPKForA(maxItems:Int):String = {
		val rand = new java.util.Random
		"key-" + (rand.nextInt(maxItems) + 1)
	}
	
	// secondary key
	protected def getRandomSKForA(maxItems:Int):String = {
		val rand = new java.util.Random
		"key2-" + (rand.nextInt(maxItems) + 1)
	}
	
	
	// Benchmarking functions
	protected def benchmarkOp1(maxItems:Int)(implicit env: Environment) = {
		val rand = new java.util.Random
		(new A).singleGet("ent_A", StringField.apply(getRandomPKForA(maxItems)), ReadRandomPolicy)
		// add 1 b/c nextInt => [0,maxItems), but I want [1,maxItems]
	}

	// maxItems => which A should be the owner of all the B's; 
	// numB => how many B's should I return
	protected def benchmarkOp2(numB:Int, maxItems:Int)(implicit env: Environment) = {
		(new A).prefixGet("ent_B", StringField.apply(getRandomPKForA(maxItems)), IntegerField.apply(numB), true, ReadRandomPolicy)
	}
	
	// maxItems => what to use for the secondary key;
	// numA => what to use for the limit (= how many of the entries sharing this SK should I return)
	protected def benchmarkOp3(numA:Int, maxItems:Int)(implicit env: Environment) = {
		// Generate TupleStream
		val secondaryKey = getRandomSKForA(maxItems)
		var tupleStream:List[Tuple] = Nil
		(1 to numA).foreach((i) => {
			val primaryKey = "key-" + i
			val field = new CompositeField(StringField.apply(secondaryKey) :: StringField.apply(primaryKey) :: Nil, 
				classOf[StringField].asInstanceOf[Class[Field]] :: classOf[StringField].asInstanceOf[Class[Field]] :: Nil)
			val newTuple = (new Tuple(field, new IntegerVersion, StringField.apply(primaryKey).serializeKey))

			tupleStream = newTuple :: tupleStream
		})
		tupleStream = tupleStream.reverse
		
		// Call op
		try {
			(new A).sequentialDereferenceIndex("ent_A", ReadRandomPolicy, tupleStream)
		} catch {
			case e:AssertionError => e.printStackTrace()
		}
	}
	
	// doesn't need maxItems b/c assumes that numA & numB are less than maxItems;
	// numA => length of EntityStream;
	// numB => # of B's to be returned for each A;
	// aSize => #chars in each A's dataA field
	protected def benchmarkOp4(aSize:Int, numA:Int, numB:Int)(implicit env: Environment) = {
		// Generate EntityStream
		var entityStream:List[A] = Nil

		(1 to numA).foreach((i) => {
			val a = new A
			a.key("key-" + i)
			a.key2("key2-" + i)
			a.dataA(PerOpDataGen.generateDataField(aSize))

			entityStream = a :: entityStream
		})
		entityStream = entityStream.reverse
		
		// Call op
		(new A).prefixJoin("ent_B", List(AttributeCondition("key")), IntegerField.apply(numB), true, ReadRandomPolicy, entityStream)
	}


	
}
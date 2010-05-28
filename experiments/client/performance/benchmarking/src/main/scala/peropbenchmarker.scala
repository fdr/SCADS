package edu.berkeley.cs.scads.benchmarking

//import edu.berkeley.cs.scads.model._
import edu.berkeley.cs.scads.piql._
import edu.berkeley.cs.scads.piql.parser._

import piql._

import org.apache.log4j._
import org.apache.log4j.Level._
//import edu.berkeley.cs.scads.thrift._
import java.lang.Math
import scala.collection.mutable._

import org.apache.avro.specific.SpecificRecordBase


object PerOpBenchmarker extends QueryExecutor {
	def benchmarkOp(opType:Int, aSize:Int, bSize:Int, numA:Int, numB:Int, maxItemsA:Int, maxItemsB:Int)(implicit env: Environment) = {
		opType match {
			case 1 => benchmarkOp1(maxItemsA)
			case 2 => benchmarkOp2(numB, maxItemsA)
			case 3 => benchmarkOp3(numA, maxItemsA)
			case 4 => benchmarkOp4(aSize, numA, numB)
			case 5 => benchmarkOp5(aSize, bSize, numB, maxItemsA, maxItemsB)
			case 6 => benchmarkOp6(aSize, numA, maxItemsA)
			case 7 => benchmarkOp7(aSize, numA, maxItemsA)
			case 8 => benchmarkOp8(aSize, numA, maxItemsA)
			case 9 => benchmarkOp9(aSize, numA, maxItemsA)
		}
		
	}
	
	
	// Util functions
	protected def getRandomPKForA(maxItems:Int):String = {
		val rand = new java.util.Random
		"key1-" + (rand.nextInt(maxItems) + 1)
	}
	
	// secondary key
	protected def getRandomSKForA(maxItems:Int):String = {
		val rand = new java.util.Random
		"key2-" + (rand.nextInt(maxItems) + 1)
	}
	
	
	// Benchmarking functions
	protected def benchmarkOp1(maxItems:Int)(implicit env: Environment) = {
		//singleGet("ent_A", StringField.apply(getRandomPKForA(maxItems)), ReadRandomPolicy)
		singleGet("ent_A", List(BoundStringValue( getRandomPKForA(maxItems) )))
	}

	// maxItems => which A should be the owner of all the B's; 
	// numB => how many B's should I return
	protected def benchmarkOp2(numB:Int, maxItems:Int)(implicit env: Environment) = {
		//prefixGet("ent_B", StringField.apply(getRandomPKForA(maxItems)), IntegerField.apply(numB), true, ReadRandomPolicy)
		val key = new A.KeyType
		key.key1 = getRandomPKForA(maxItems)
		prefixGet("ent_B", List(BoundAvroRecordValue( key )), BoundIntegerValue(numB), true)
		
	}
	
	
	// CAN'T be used till indexes are created
	// When it works, figure out how to creat the tupleStream 
	// How? call userByEmail and see what prefixGet returns 
	//
	// maxItems => what to use for the secondary key;
	// numA => what to use for the limit (= how many of the entries sharing this SK should I return)
	protected def benchmarkOp3(numA:Int, maxItems:Int)(implicit env: Environment) = {
		/*
		// Generate TupleStream
		val secondaryKey = getRandomSKForA(maxItems)
		//var tupleStream:List[Tuple] = Nil
		var tupleStream:List[(SpecificRecordBase, SpecificRecordBase)] = Nil
		
		(1 to numA).foreach((i) => {
			val primaryKey = "key-" + i
			val field = new CompositeField(StringField.apply(secondaryKey) :: StringField.apply(primaryKey) :: Nil, 
				classOf[StringField].asInstanceOf[Class[Field]] :: classOf[StringField].asInstanceOf[Class[Field]] :: Nil)
			//val newTuple = (new Tuple(field, new IntegerVersion, StringField.apply(primaryKey).serializeKey))

			//tupleStream = newTuple :: tupleStream
			tupleStream = ()
		})
		tupleStream = tupleStream.reverse
		
		// Call op
		try {
			//sequentialDereferenceIndex("ent_A", ReadRandomPolicy, tupleStream)
			sequentialDereferenceIndex("ent_A", tupleStream)
		} catch {
			case e:AssertionError => e.printStackTrace()
		}
		*/
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
			a.key1 = ("key1-" + i)
			a.key2 = ("key2-" + i)
			a.dataA = PerOpDataGen.generateDataField(aSize)

			entityStream = a :: entityStream
		})
		entityStream = entityStream.reverse
		
		// Call op
		//prefixJoin("ent_B", List(AttributeCondition("key")), IntegerField.apply(numB), true, ReadRandomPolicy, entityStream)
		prefixJoin("ent_B", List(AttributeCondition("key1")), BoundIntegerValue(numB), true, entityStream)
		
	}

	/*
	// aSize, bSize => length of dataA & dataB fields (respectively);
	// numB => length of entityStream;
	// maxItemsA&B => how to set key, key2
	(1 to maxItemsA).foreach((i) => {
		val a = new A
		a.key1 = ("key1-" + i)
		a.key2 = ("key2-" + i)
		a.dataA = generateDataField(aSize)
		a.save
		
		(1 to maxItemsB).foreach((j) => {
			val b = new B
			b.owner = a
			b.key2 = ("key2-" + j)
			b.dataB = generateDataField(bSize)
			b.save

			//if ((j % 100) == 0)
				println("Added " + j + " items of type B, owned by A:key" + i + "...")
		})
		
		//if ((i % 100) == 0)
			println("Added " + i + " items of type A...")
	})
	*/
	
	protected def benchmarkOp5(aSize:Int, bSize:Int, numB:Int, maxItemsA:Int, maxItemsB:Int)(implicit env: Environment) = {
		var entityStream:List[B] = Nil

		println("Namespace has data? " + env.namespaces("ent_A").getRange(null, null))

		// Generate EntityStream
		(1 to numB).foreach((i) => {
			val a = new A
			//val key = getRandomPKForA(maxItemsA)
			val keyNum = ((new java.util.Random).nextInt(maxItemsA) + 1)
			a.key1 = ("key1-" + keyNum)
			a.key2 = ("key2-" + keyNum)
			a.dataA = PerOpDataGen.generateDataField(aSize)

			val b = new B
			b.owner = a
			b.key2 = getRandomSKForA(maxItemsB)	// same format as for A's SK
			b.dataB = PerOpDataGen.generateDataField(bSize)

			entityStream = b :: entityStream
		})
		entityStream = entityStream.reverse	

		// Call Op
		//pointerJoin("ent_A", List(AttributeCondition("owner")), ReadRandomPolicy, entityStream)
		pointerJoin("ent_A", List(AttributeCondition("owner")), entityStream)
		
	}
	
	
	// aSize => length of dataA field;
	// numA => length of TupleStream;
	// maxItems => what to use for A's key, key2
	protected def benchmarkOp6(aSize:Int, numA:Int, maxItems:Int)(implicit env: Environment) = {
		//var tupleStream:List[Tuple] = Nil
		//var tupleStream:TupleStream = Nil
		var tupleStream:List[(SpecificRecordBase, SpecificRecordBase)] = Nil
		
		// Generate TupleStream
		(1 to numA).foreach((i) => {
			val a = new A
			a.key1 = getRandomPKForA(maxItems)
			a.key2 = getRandomSKForA(maxItems)
			a.dataA = PerOpDataGen.generateDataField(aSize)
			
			//tupleStream = (new Tuple(a.key1, new IntegerVersion, a.serializeAttributes)) :: tupleStream 
			tupleStream = (a.key, a.value) :: tupleStream
		})
		tupleStream = tupleStream.reverse
		
		// Call Op
		materialize(classOf[A].asInstanceOf[Class[edu.berkeley.cs.scads.piql.Entity[_,_]]], tupleStream)
	}
	
	
	// aSize => length of dataA field;
	// numA => length of child;
	// maxItems => how to choose PK, SK
	// How to sample from this model?  You wouldn't know the selectivity of the op.  Does that matter?
	protected def benchmarkOp7(aSize:Int, numA:Int, maxItems:Int)(implicit env: Environment) = {
		// Generate random fraction
		val rand = new java.util.Random
		val matchFraction = rand.nextInt(100)/100.0
		val numMatchingEntities = Math.floor(matchFraction * numA)
		println("match fraction = " + matchFraction)

		// Create child:Seq[Entity]
		val sharedSK = getRandomSKForA(maxItems)	// make sure the other SKs are NOT this
		var whichEntitiesShareSK:List[Int] = Nil

		while (whichEntitiesShareSK.length < numMatchingEntities) {
			val idx = rand.nextInt(numA) + 1
			if (!whichEntitiesShareSK.exists(s => s == idx))
				whichEntitiesShareSK = idx :: whichEntitiesShareSK
		}
		// sort?
		
		var aEntitySeq:List[A] = Nil

		(1 to numA).foreach((i) => {
			val a = new A
			a.key1 = ("key-" + i)
			if (whichEntitiesShareSK.exists(s => s == i))
				a.key2 = sharedSK
			else
				a.key2 = "key2-other"
			a.dataA = PerOpDataGen.generateDataField(aSize)

			aEntitySeq = a :: aEntitySeq
		})
		aEntitySeq = aEntitySeq.reverse
		
		// Call op
		selection(HashMap(("key2", BoundStringValue(sharedSK))).asInstanceOf[HashMap[String,BoundValue]], aEntitySeq)
		
	}
	
	
	// aSize => length of dataA field;
	// numA => length of child;
	// maxItems => how to choose PK, SK
	protected def benchmarkOp8(aSize:Int, numA:Int, maxItems:Int)(implicit env: Environment) = {
		// Generate child:Seq[Entity]
		// PK's should be in random order.
		
		var pkList:List[String] = Nil
		var aEntitySeq:List[A] = Nil
		
		while (pkList.length < numA) {
			val pk = getRandomPKForA(maxItems)
			if (!pkList.exists(s => s == pk)) {
				pkList = pk :: pkList
				
				val a = new A
				a.key1 = pk
				a.key2 = getRandomSKForA(maxItems)
				a.dataA = PerOpDataGen.generateDataField(aSize)
				
				aEntitySeq = a :: aEntitySeq
			}
		}
		
		// Call op
		sort(List("key1"), true, aEntitySeq)
	}


	// aSize => length of dataA field;
	// numA => length of child;
	// maxItems => how to choose PK, SK
	protected def benchmarkOp9(aSize:Int, numA:Int, maxItems:Int)(implicit env: Environment) = {
		// Generate child
		var aEntitySeq:List[A] = Nil
		
		(1 to numA).foreach((i) => {
			val a = new A
			a.key1 = getRandomPKForA(maxItems)
			a.key2 = getRandomSKForA(maxItems)
			a.dataA = PerOpDataGen.generateDataField(aSize)
			
			aEntitySeq = a :: aEntitySeq
		})
		aEntitySeq = aEntitySeq.reverse
		
		// Choose random K from [1,numA]
		val K = (new java.util.Random).nextInt(numA) + 1 	// add 1 so range is [1,numA]; otherwise, it's [0,numA)
		
		// Call op
		//topK(IntegerField(K), aEntitySeq)
		topK(BoundIntegerValue(K), aEntitySeq)
	}
	
}
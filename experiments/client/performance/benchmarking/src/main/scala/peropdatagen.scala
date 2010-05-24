//import edu.berkeley.cs.scads.model._
import edu.berkeley.cs.scads.piql._
import edu.berkeley.cs.scads.piql.parser._

import org.apache.log4j._
import org.apache.log4j.Level._
//import edu.berkeley.cs.scads.thrift._

object PerOpDataGen {
	def genData(opType:Int, aSize:Int, bSize:Int, maxItemsA:Int, maxItemsB:Int)(implicit env: Environment) = {
		opType match {
			case 1 => datagenOp1(aSize, maxItemsA)
			case 2 => datagenOp2(aSize, bSize, maxItemsA, maxItemsB)
			case 3 => datagenOp3(aSize, maxItemsA)
			case 4 => datagenOp4(aSize, bSize, maxItemsA, maxItemsB)
			case 5 => datagenOp5(aSize, bSize, maxItemsA, maxItemsB)
			case 6 => /* no datagen necessary */
			case 7 => /* no datagen necessary */
			case 8 => /* no datagen necessary */
			case 9 => /* no datagen necessary */
		}
	}
	
	// Datagen functions
	def generateDataField(length:Int):String = {
		var charList:List[String] = Nil

		(1 to length).foreach((i) => {
			charList = "a" :: charList
		})
		
		charList.mkString("")
	}
	
	protected def generateManyBsPerA(aSize:Int, bSize:Int, maxItemsA:Int, maxItemsB:Int)(implicit env: Environment) = {
		println("Generating data for op2=prefixGet...")
		(1 to maxItemsA).foreach((i) => {
			val a = new A
			a.key("key-" + i)
			a.key2("key2-" + i)
			a.dataA(generateDataField(aSize))
			a.save
			
			(1 to maxItemsB).foreach((j) => {
				val b = new B
				b.owner(a)
				b.key2("key2-" + j)
				b.dataB(generateDataField(bSize))
				b.save

				if ((j % 100) == 0)
					println("Added " + j + " items of type B, owned by A:key" + i + "...")
			})
			
			if ((i % 100) == 0)
				println("Added " + i + " items of type A...")
		})
	}
	
	protected def datagenOp1(aSize:Int, maxItemsA:Int)(implicit env: Environment) = {
		println("Generating data for op1=singleGet...")
		(1 to maxItemsA).foreach((i) => {
			val a = new A
			a.key("key-" + i)
			a.key2("key2-" + i)
			a.dataA(generateDataField(aSize))
			a.save
			
			if ((i % 100) == 0)
				println("Added " + i + " items...")
		})
	}
	
	protected def datagenOp2(aSize:Int, bSize:Int, maxItemsA:Int, maxItemsB:Int)(implicit env: Environment) = {
		generateManyBsPerA(aSize, bSize, maxItemsA, maxItemsB)
	}
	
	protected def datagenOp3(aSize:Int, maxItems:Int)(implicit env: Environment) = {
		println("Generating data for op3=sequentialDereferenceIndex...")
		(1 to maxItems).foreach((i) => {
			val secondaryKey = "key2-" + i
			
			(1 to maxItems).foreach((j) => {
				val a = new A
				a.key("key-" + ((i-1)*maxItems + j))
				a.key2(secondaryKey)
				a.dataA(generateDataField(aSize))
				a.save
				
				if ((j % 100) == 0)
					println("Added " + j + " items of type A with secondary key " + secondaryKey)
			})
		})
	}
	
	protected def datagenOp4(aSize:Int, bSize:Int, maxItemsA:Int, maxItemsB:Int)(implicit env: Environment) = {
		generateManyBsPerA(aSize, bSize, maxItemsA, maxItemsB)
	}

	protected def datagenOp5(aSize:Int, bSize:Int, maxItemsA:Int, maxItemsB:Int)(implicit env: Environment) = {
		generateManyBsPerA(aSize, bSize, maxItemsA, maxItemsB)
	}
	
	// No datagen necessary for ops 6-9
	
}
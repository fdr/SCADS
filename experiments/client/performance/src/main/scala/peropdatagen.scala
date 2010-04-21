import edu.berkeley.cs.scads.model._
import org.apache.log4j._
import org.apache.log4j.Level._
import edu.berkeley.cs.scads.thrift._

object PerOpDataGen {
	def genData(opType:Int, aSize:Int, bSize:Int, maxItems:Int)(implicit env: Environment) = {
		opType match {
			case 1 => datagenOp1(aSize, maxItems)
			case 2 => datagenOp2(aSize, bSize, maxItems)
			case 3 => datagenOp3(aSize, maxItems)
			case 4 => datagenOp4(aSize, bSize, maxItems)
			/*
			case 5 => datagenOp5(aSize, bSize, maxItems)
			case 6 => datagenOp6(aSize, bSize, maxItems)
			case 7 => datagenOp7(aSize, bSize, maxItems)
			case 8 => datagenOp8(aSize, bSize, maxItems)
			case 9 => datagenOp9(aSize, bSize, maxItems)
			*/
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
	
	protected def generateManyBsPerA(aSize:Int, bSize:Int, maxItems:Int)(implicit env: Environment) = {
		println("Generating data for op2=prefixGet...")
		(1 to maxItems).foreach((i) => {
			val a = new A
			a.key("key-" + i)
			a.key2("key2-" + i)
			a.dataA(generateDataField(aSize))
			a.save
			
			(1 to maxItems).foreach((j) => {
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
	
	protected def datagenOp1(aSize:Int, maxItems:Int)(implicit env: Environment) = {
		println("Generating data for op1=singleGet...")
		(1 to maxItems).foreach((i) => {
			val a = new A
			a.key("key-" + i)
			a.key2("key2-" + i)
			a.dataA(generateDataField(aSize))
			a.save
			
			if ((i % 100) == 0)
				println("Added " + i + " items...")
		})
	}
	
	protected def datagenOp2(aSize:Int, bSize:Int, maxItems:Int)(implicit env: Environment) = {
		generateManyBsPerA(aSize, bSize, maxItems)
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
	
	protected def datagenOp4(aSize:Int, bSize:Int, maxItems:Int)(implicit env: Environment) = {
		generateManyBsPerA(aSize, bSize, maxItems)
		
	}
	
}
/*(List(1)).foreach((i) => println(i))

var r:Seq[Int] = null

if (true) {
	r = 0 to 10 by 2
	r.foreach(println(_))
} else {
	r = List(1)
	r.foreach(println(_))	
}


r = 0 to 10 by 2
r.foreach(println(_))


r = List(1)
r.foreach(println(_))	

*/

val flag = "true".toBoolean
if (flag)
	println("yes")
println(flag)
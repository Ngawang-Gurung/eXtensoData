import scala.io.StdIn.readLine

// Method
def sum(x: Int, y: Int): Int = x + y

// Class
class Greeter(prefix: String, suffix: String):
  def greet(name: String): Unit =
    println(prefix + name + suffix)

@main
def hello(): Unit = {     // hello is method and declared as a "main" method with @main annotation
  println("Hello world!")

  // Variables and Datatypes

  // val is immutable // rule of thumb in Scala is to always use val unless the variable specifically needs to be mutated
  val z: Int = 1+1
  println(z)

  // var is mutable
  var language: String = "Python"   // Scala suport string in double quotes only
  language = "Scala"
  println(s"My new language is $language") // String Interpolation

  // Other datatypes include: Byte, Long, Short, Double, Float

  // User Input
//  println("Enter your name:")
//  val username = readLine()
//  println("Hello, " + username + "!" ) // String Concatenation

  // If, else, if
  val a: Int = 20 // Explicit
  val b = 2 // Implicit
  val c = 10

  if a > 10 then
    println(s"${a * b}")
  else if (a == 0) then
    println("zero")
  else
    println(s"Default = $c")

  // If, else clause as as an expression
  val x = if a > 10 then a * b else c
  println(s"Using if-else expression, = $x")

  // Match
  val i = 1
  val result = i match
    case 1 => "one"
    case 2 => "two"
    case _ => "other"

  // List
  val myList = List(1, 2, 3)

  // Dictionary/Map
  val dict = Map(
    "Toy Story" -> 8.3,
    "Forrest Gump" -> 8.8
  )

  // Set
  val mySet = Set(1, 2, 3)

  // Tuple
  val myTuple = (11, "Eleven")

  // For loop
//  for i <- 1 to 3 do println(i) // 3 is inclusive
//  for i <- 1 until 3 do println(i) // 3 is exclusive

  // For loop with Iterables
  for i <- myList if i>2 do println(i)  // If expression inside for loop is called guards.

  // For loop, multiple "range" generators // Multiple for-loop
  for
    i <- 1 to 2
    j <- 3 to 4
  do
    println(s"i=$i, j=$j")

  // Comprehension
  val doubles = for i <- myList yield i * 2
  println(doubles)

  // While loop
  var t = 1
  while t < 3 do
    println(t)
    t += 1

  // Try, catch, finally
  try
    val result = 10 / 0 // This will throw an ArithmeticException
    println("Result: " + result) // This line will not execute
  catch
    case e: ArithmeticException => println("ArithmeticException caught: " + e.getMessage)
  finally
    println("Cleanup code here.")

  // Function
  val add = (x: Int, y: Int) => x + y
  println(add(100, 200))

  // Map is a higher-order function - a function that takes a function as parameter
  val twice = List(1, 2, 3).map(i => i * 2)

  // Calling method
  println(sum(1, 2)) // 3

  // Creating object
  val greeter = Greeter("Hello ", "!")
  greeter.greet("Scala developer")
}










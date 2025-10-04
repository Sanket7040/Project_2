package Spark_Practice_1
import scala.::
import scala.annotation.tailrec
import scala.io.StdIn
object Scala_Functions {
  //annnomys function= it is function with no name.it is ude for simple & short task called annmoys function
  def main(args:Array[String]):Unit={
    val square=(x:Int)=>x*x

    println(square(5))

    //high order function=the
    def applyfunc(f:Int=>Int,x:Int):Int={
    f(x)
    }
    val square1=(x:Int)=>x*x

    val result=applyfunc(square1,5)
    println(result)

    //recusrsive fuction

    def factorial(n:Int): Int = {
      if(n==0) 1
      else n*factorial(n-1)
    }
    println(factorial(5))

// tailrecurstion
//1. factorial
    def fact(n:Int):Int={
      @tailrec
      def loop (x:Int,acc:Int):Int={
        if (x<=1)acc
        else loop (x-1,x*acc)
      }
   loop(n,1)

    }
    println(fact(5))

    //2.fibanoci series

    def fibanocii(n:Int):Int={
      @tailrec
      def loop1(n:Int,a:Int,b:Int):Int={
        if(n==0)a
        else loop1(n-1,b,a+b)

      }
      loop1(n,0,1)
    }
    println(fibanocii(7))
//currying function
    def multiply(a:Int,b:Int):Int=a+b

    val result1=multiply(10,_)

    println(result1(15))

    //partially apply function

    def add(a:Int,b:Int,c:Int):Int=a+b+c

    val additon=add(5,6,_:Int)

    println(additon(5))

    //closure function

    var factor=10

    def closure_fuc(a:Int):Int=a*factor
    println(closure_fuc(5))

//    var arr=Array.ofDim[Int](5,5)
//
//    arr(0)(0)=0
//    arr(0)(1)=2
//
//    println(arr.foreach(0)())
// val num=List(1,2,3)
//  val doubled=num.flatMap(x=>(x,x)
//    println(doubled)

    def reverselist (lst:List[Int]):List[Int]={
      lst.foldLeft(List[Int]())((acc,x) =>x :: acc)

    }
    println(reverselist(List(1,2,3,4,5)))

val num=List(1,2,3,4,5,6,1,2,1)
   val frequency=num.groupBy(identity).mapValues(_.size)
    println(frequency)

    val n=25

    def isprimecheck (n:Int):Boolean={
      if (n<=1) false
     for (i<- 2 until(n)){
       if (n%2==0) false
     }
 true

    }
    if(isprimecheck(n)){
      println("this is prime number")

    }

    println("enter the number")
    val a=StdIn.readInt()

    for (i<-1 to a){
      val squre=i*i
      println(squre)
    }



  }

}

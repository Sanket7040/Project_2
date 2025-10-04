package Spark_Practice_1

import scala.io.StdIn

object prime_number {
  def main(args:Array[String]):Unit={

    println("Enter a value")
    val num=StdIn.readInt()


    // function to check num
    def isprime(n:Int):Boolean= {
      if(n<=1) return false
      for(i<-2 until(n)){
        if(n%2==0) false
      }
      true
    }

    if(isprime(num)){
      println(s"$num is prime number")
    }
    else {
      println(s"$num is not prime number")





    }

  }

}

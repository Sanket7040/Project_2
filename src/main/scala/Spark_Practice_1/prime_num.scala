package Spark_Practice_1

import org.apache.commons.math3.util.FastMath.sqrt
import org.apache.hadoop.yarn.webapp.hamlet2.HamletSpec.InputType

import scala.io.StdIn
import scala.math.BigDecimal.double2bigDecimal

object prime_num {

  def main(args:Array[String]):Unit={
    println("Enter the number")
    var num=StdIn.readInt()

    def primechcek(n:Int):Boolean={
      if(n<=1) return false
      for (i <- 2  to math.sqrt(n).toInt ){
        if(n%i==0) return false
      }
      true
    }

    if(primechcek(num)){
      println("this is prime number")
    }
    else {
      println("not a prime number")
    }

  }

}

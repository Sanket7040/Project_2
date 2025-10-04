package Spark_Practice_1

import scala.io.StdIn

object ArraY_prct12 {
def main (args:Array[String]):Unit={
  print("enter the size of array: ")
  var num=StdIn.readInt()

  var arr=new Array[Int](num)

  for (i <- 0 until num){
    println(s"Enter the array value: arr(${i+1})")
    arr(i)=StdIn.readInt()

  }
  println("diaplaying the max_arr_element")

 println(arr.max)

  println("displaying min arrya value:")
  println(arr.min)
}
}

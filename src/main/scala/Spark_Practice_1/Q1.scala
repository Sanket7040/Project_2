package Spark_Practice_1

import scala.io.StdIn

object Q1 {
  def main(args: Array[String]): Unit = {
    print("enter the size of array:")
      var num=StdIn.readInt()

    var arr=new Array[Int](num)

    for (i<-0 until num){
    println(s"Enter the array value: arr(${i+1})")
      arr(i)=StdIn.readInt()

    }
    println("Enter the element you search:")
    var searchelement=StdIn.readInt()
if (arr.contains(searchelement)){
  println("the vakue is present")}

else println("the value is not present")

}




}

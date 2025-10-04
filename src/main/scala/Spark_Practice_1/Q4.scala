package Spark_Practice_1

import scala.io.StdIn

object Q4 {
  def main (args:Array[String]):Unit={
    print("Enter the size of row1: ")
    var row1=StdIn.readInt()
    print("Enter the size of column1: ")
    var col1=StdIn.readInt()

    var arr1=Array.ofDim[Int](row1,col1)

    for (i<-0 until row1) {
      for (j <- 0 until col1) {
        println(s"Enter the value of matrix1 : arr[$i][$j]")
        arr1(i)(j)=StdIn.readInt()
      }
    }

      println("display the matrix1: ")
      for (i<- 0 until row1){
        for (j<- 0 until col1){
          print(arr1(i)(j) + " ")
        }
        println()

      }
    println("Enter the value for matrix 2")
    print("Enter the size for row2: ")
     val row2=StdIn.readInt()
    print("Enter the size for col2 ")
    val col2=StdIn.readInt()

    val arr2=Array.ofDim[Int](row2,col2)

    for(i<- 0 until row2) {
      for (j <- 0 until  col2 ){
  println(s"Enter the value for matrix 2: arr2[$i][$j]")
        arr2(i)(j)=StdIn.readInt()

        }

    }
    for (i<-0 until row1){

    }


  }

}

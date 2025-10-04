package Spark_Practice_1

import com.google.gson.annotations.Until

import scala.io.StdIn

object Q3 {
  def main(args: Array[String]): Unit = {
    println("Enter the size of row :")
    var row = StdIn.readInt()
    println("Enter the column size: ")
    var col = StdIn.readInt()

    var arr = Array.ofDim[Int](row, col)

    for (i <- 0 until row) {
      for (j <- 0 until col) {
        println(s"Enter the value of array: arr[$i][$j]")
        arr(i)(j) = StdIn.readInt()
      }
    }
    println("Diplay the array value: ")
    for (i <- 0 until row) {
      for (j <- 0 until col) {
        print(arr(i)(j) + " ")
      }

      println()


    }
  }


}

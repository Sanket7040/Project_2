package Spark_Practice_1

import scala.annotation.tailrec

object fuction {
  def main(args: Array[String]): Unit = {
    //Annoymous function
    val square = (x: Int) => x * x
    println(square(5))
    //high order function
    val square1 = (x: Int) => x * x

    def applyfun(funcn: Int => Int, x: Int): Int = {
      funcn(x)

    }

    val result = applyfun(square1, 2)
    println(result)

    //recursuve function
    def factorail(n: Int): Int = {
      if (n == 0) 1
      else n * factorail(2)
    }

    println(factorail(5))

    //tailrecurstion

    def fib(n: Int): Int = {
      @tailrec
      def loop1(n: Int, a: Int, b: Int): Int = {
        if (n == 0) a
        else loop1(n - 1, b, a + b)

      }

      loop1(n, 0, 1)
    }

    println(fib(5))


  }
}

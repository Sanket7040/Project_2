import scala.io.StdIn
import scala.io.StdIn.readInt

object Main {
  def main(args: Array[String]): Unit = {
    println("Enter the size of array")
    val num=readInt

    val arr=new Array[Int](num)


    for ( i<- 0 until num){
      println(s"Enter the element in array:arr${i+1}")
      arr(i)=StdIn.readInt()
    }
    println("diplaying the eelments")
    arr.foreach(println)



      }

}
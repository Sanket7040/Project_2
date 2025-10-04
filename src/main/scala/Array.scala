import scala.io.StdIn

object Array {
  def main(args: Array[String]): Unit = {
    println("Enter the size of array: ")
    var num=StdIn.readInt()

    var arr=new Array [Int](num)


    for (i<-0 until num){
    println(s"enter the array element : arr${i+1}")
     arr(i)=StdIn.readInt()
     var sum =arr.sum
      println(sum)
    }




  }

}

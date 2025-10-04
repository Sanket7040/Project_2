import org.apache.spark.sql.SparkSession

object spark_1 {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().master("local").appName("spark.test").getOrCreate()

  val df =spark.read.csv("C:\\Users\\saksh\\IdeaProjects\\Spark_try\\src\\main\\scala\\Input_file\\emp.csv")
    df.show()

    df.write.csv("C:\\Users\\saksh\\IdeaProjects\\Spark_try\\Output\\project1")


  }

}

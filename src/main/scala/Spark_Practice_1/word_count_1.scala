package Spark_Practice_1

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object word_count_1 {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().master("local").appName("word_count").getOrCreate()

    val sc=spark.sparkContext
    sc.setLogLevel("error")

    val input_rdd=sc.textFile("C:\\Users\\saksh\\IdeaProjects\\Spark_try\\src\\main\\scala\\Input_file\\word_count_2")

    val split_line_rdd=input_rdd.flatMap(line => line.split(" "))
    val word_split_rdd=split_line_rdd.map(word => (word,1))
    val word_count=word_split_rdd.reduceByKey(_+_)
    word_count.collect().foreach(println)

    spark.stop()


  }

}

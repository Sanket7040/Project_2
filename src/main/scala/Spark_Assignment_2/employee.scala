package Spark_Assignment_2

import org.apache.spark.SparkContext._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, count, max, min, sum}
import org.apache.spark.sql.types.{DecimalType, IntegerType, StringType, StructField, StructType}

object employee {
  def main(args: Array[String]): Unit = {
    val Spark=SparkSession.builder().master("local").appName("read_csv").getOrCreate()

    val sc=Spark.sparkContext
    sc.setLogLevel("error")

    val df1=Spark.read.option("header",true).option("InferSchema",true).
      csv("C:\\Users\\saksh\\IdeaProjects\\Spark_try\\src\\main\\scala\\Input_file\\employee.csv")
      df1.show()
      df1.printSchema()

    val employee=StructType(Array(StructField("first_name",StringType,true),
      StructField("last_name",StringType,true),
      StructField("gender",StringType,true),
      StructField("age",IntegerType,true),
      StructField("salary",DecimalType(10,2),true),
      StructField("dept",StringType,true),
        StructField("city",StringType,true)))

    val df=Spark.read.option("header",true).option("InferSchema",true).schema(employee).
      csv("C:\\Users\\saksh\\IdeaProjects\\Spark_try\\src\\main\\scala\\Input_file\\employee.csv")
    df.show()
    df.printSchema()

    println("1.1. Find the total salary of employees in each department.")
    df.groupBy(col("dept")).agg(sum(col("salary")).alias("total_salary")).show()

    println("2. Count the number of employees in each department.")
    df.groupBy(col("dept")).agg(count("*").alias("no_of_employees")).show()

    println("3. Find the average salary of employees in each city.")
    df.groupBy(col("city")).agg(avg("salary").alias("avg_salary")).show()

    println("4. Find the maximum and minimum salaries in each department.")
    df.groupBy(col("dept")).agg(max("salary").alias("max_salary"),min("salary").alias("min_salary")).show()

    println("5. Count the number of male and female employees in each department.")
    df.groupBy(col("dept")).agg(count("gender").alias("male_count"),count("gender").alias("female_count")).show()

    println("6. Find the total salary of employees in each city, but only include cities with a total salary greater than 200,000.")
    df.groupBy(col("city")).agg(sum("salary").alias("total_salary")).filter(col("total_salary")>200000).show()

println("7. Find the average age of employees in each department.")
    df.groupBy(col("dept")).agg(avg(col("age")).alias("avg_age")).show()

println("8. Count the number of employees in each department who have a salary greater than 70,000.")
    df.filter(col("salary")>70000).groupBy(col("dept")).agg(count("*").alias("no_of_employees")).show()

    println("9. Find the total number of employees in each city.")
    df.groupBy(col("city")).agg(count("*").alias("no_of_employees")).show()

    println("10. Find the average salary of employees in each department, but only include departments with more than 5 employees.")
    df.groupBy("dept").agg(avg("salary")alias("avg_salary"),count("*").alias("no_of_employees")).filter(col("no_of_employees")>5).show()

    println("11. Find the maximum age of employees in each department.")
    df.groupBy(col("dept")).agg(max(col("age")).alias("max_age")).show()

    println("12. Count the number of employees in each city who are older than 30.")
    df.filter(col("age")>30).groupBy(col("city")).agg(count("*").alias("no_of_employees")).show()

    println("13. Find the total salary of employees in each city, but only include cities with more than 3 employees.")
    df.groupBy(col("city")).agg(sum("salary").alias("total_salary"),count("*").alias("emp_count")).filter(col("emp_count")>3).show()

    println("14. Find the average salary of male employees in each department.")
    df.filter(col("gender")==="Male").groupBy(col("dept")).agg(avg("salary").alias("avg_salary")).show()

    println("15. Count the number of employees in each city who have a salary less than 60,000.")
    df.filter(col("salary")<60000).groupBy(col("city")).agg(count("*").alias("no_of_employees")).show()

    println("16. Find the total salary of female employees in each department.")
    df.filter(col("gender")==="Female").groupBy(col("dept")).agg(sum("salary").alias("total_salary")).show()

    println("17. Find the average age of employees in each city.")
    df.groupBy(col("city")).agg(avg("age").alias("avg_age")).show()

    println("18. Count the number of employees in each department who are younger than 28.")
    df.filter(col("age")<28).groupBy(col("dept")).agg(count("*").alias("no_of_employees")).show()

    println("19. Find the maximum and minimum salaries in each city.")
    df.groupBy(col("city")).agg(max("salary").alias("max_salary"),min("salary").alias("min_salary")).show()

    println("20.Count the number of male and female employees in each city.")
    df.groupBy(col("city")).agg(count("gender").alias("male_count"),count("gender").alias("female_count")).show()

    println("21. Find the total salary of employees in each department, but only include departments with a total salary greater than 300,000.")
    df.groupBy(col("dept")).agg(sum(col("salary")).alias("total_salary")).filter(col("total_salary")>300000).show()

    println("22. Find the average age of male and female employees in each department.")
    df.groupBy("dept","gender").agg(avg(col("age")).alias("avg_age")).show()

    println("23. Count the number of employees in each city who have a salary greater than 70,000.")
    df.filter(col("salary")>70000).groupBy("city").agg(count("*")).alias("no_of_employees").show()

    println("24. Find the total salary of employees in each city, but only include cities with a total salary less than 300,000.")
    df.groupBy(col("city")).agg(sum("salary").alias("total_salary")).filter(col("total_salary")<30000).show()

    println("25. Find the average salary of female employees in each department.")
    df.filter(col("gender")==="Female").groupBy(col("dept")).agg(avg(col("salary")).alias("avg_salary")).show()

    println("26. Count the number of employees in each department who are older than 30.")
    df.filter(col("age")>30).groupBy("dept").agg(count("*")).alias("no_of_employees").show()

    println("27. Find the total salary of employees in each city, but only include cities with more than 5 employees.")
    df.groupBy(col("city")).agg(sum("salary").alias("total_salary"),count("*").alias("emp_count")).filter(col("emp_count")>5).show()

    println("28.Find the average age of employees in each department who have a salary greater than 60,000.")
    df.filter(col("salary")>60000).groupBy("dept").agg(avg("age").alias("avg_age")).show()

    println("29. Count the number of employees in each city who have a salary between 60,000 and 70,000.")
    df.filter(col("salary").between(60000,70000)).groupBy("city").agg(count("*").alias("emp_count")).show()

println("30. Find the total salary of employees in each department, but only include departments with a total salary greater than 250,000.")
    df.groupBy(col("dept")).agg(sum(col("salary")).alias("total_salary")).filter(col("total_salary")>250000).show()

println("31. Find the average salary of employees in each city who are younger than 30.")
    df.filter(col("age")<30).groupBy(col("city")).agg(avg("salary").alias("avg_salary")).show()

    println("32. Count the number of employees in each department who have a salary less than 55,000.")
    df.filter(col("salary")<55000).groupBy("dept").agg(count("*").alias("emp_count")).show()

    println("33. Find the maximum and minimum ages of employees in each city.")
    df.groupBy(col("city")).agg(max("age").alias("max_age"),min("age").alias("min_age")).show()

    println("34. Count the number of male and female employees in each department who have a salary greater than 60,000.")
    df.filter(col("salary")>60000).groupBy("dept","gender").agg(count("*").alias("gender_count")).show()

    println("35. Find the total salary of employees in each city, but only include cities with a total salary greater than 150,000.")
    df.groupBy(col("city")).agg(sum("salary").alias("total_salary")).filter(col("total_salary")>150000).show()


    println("36. Find the average salary of employees in each department who are older than 30.")
df.filter(col("age")>30).groupBy("dept").agg(avg("salary").alias("avg_salary")).show()

    println("37. Count the number of employees in each city who have a salary less than 60,000.")
    df.filter(col("salary")<60000).groupBy("city").agg(count("*").alias("emp_count")).show()

    println("38.Find the total salary of female employees in each city.")
    df.filter(col("gender")==="Female").groupBy(col("city")).agg(sum("salary").alias("total_salary")).show()

    println("39.Find the average age of employees in each department who have a salary less than 60,000.")
    df.filter(col("salary")<60000).groupBy("dept").agg(avg("age").alias("avg_age")).show()

    println("40. Count the number of employees in each city who are younger than 30.")
    df.filter(col("age")<30).groupBy("city").agg(count("*").alias("emp_count")).show()

    println("41. Find the total salary of employees in each department, but only include departments with a " +
      "total salary less than 200,000.")
    df.groupBy(col("dept")).agg(sum(col("salary")).alias("total_salary")).filter(col("total_salary")<200000).show()

    println("42. Find the average salary of male employees in each city.")
    df.filter(col("gender")==="Male").groupBy("city").agg(avg("salary")).show()

    println("43. Count the number of employees in each department who have a salary greater than 75,000.")
    df.filter(col("salary")>75000).groupBy("dept").agg(count("*").alias("emp_count")).show()

    println("44. Find the total salary of employees in each city, but only include cities with more than 4 employees.")
    df.groupBy("city").agg(sum(col("salary")).alias("total_salary"),count(col("*")).alias("emp_count")).filter(col("emp_count")>4).show()

    println("45. Find the average age of female employees in each department.")
    df.filter(col("gender")==="Female").groupBy("dept").agg(avg("age").alias("avg_age")).show()

    println("46. Count the number of employees in each city who have a salary greater than 65,000.")
    df.filter(col("salary")>65000).groupBy("city").agg(count("*").alias("emp_count")).show()

    println("47. Find the total salary of employees in each department, but only include " +
      "departments with a total salary greater than 350,000.")
    df.groupBy(col("dept")).agg(sum(col("salary")).alias("total_salary")).filter(col("total_salary")>350000).show()


    println("48. Find the average salary of employees in each city who are older than 30.")
    df.filter(col("age")>30).groupBy("city").agg(avg("salary").alias("avg_salary")).show()

    println("49. Count the number of employees in each department who have a salary between 60,000 and 70,000.")
    df.filter(col("salary").between(60000,70000)).groupBy("dept").agg(count("*").alias("emp_count")).show()

    println("50. Find the maximum and minimum salaries in each department.")
    df.groupBy(col("dept")).agg(max("salary").alias("max_salary"),min("salary").alias("min_salary")).show()


  }

}

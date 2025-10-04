package Spark_Practice_1

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, count, dense_rank, lit, max, min, month, rank, row_number, sum, window}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.dsl.expressions.{DslAttr, StringToAttributeConversionHelper}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.expressions.Window


object Employee_prac {
  def main(args: Array[String]): Unit = {

    val spark=SparkSession.builder().master("local").appName("read_csv").getOrCreate()

    val sc=spark.sparkContext
    sc.setLogLevel("error")

    val emp_df=spark.read.option("header",true).option("inferSchema",true)
      .csv("C:\\Users\\saksh\\IdeaProjects\\Spark_try\\src\\main\\scala\\Input_file\\emplyoee_prc")
    emp_df.show()
    emp_df.printSchema()


///**Show only name and salary columns.
// Filter employees with salary > 60000.
// Filter employees from Delhi and IT department.
// Add a column bonus = salary * 0.1.
// Add a constant column country = India.
// Drop the column age.
// Rename salary → emp_salary.
// Count number of rows in DataFrame.
// Show distinct departments.
///**

//   df.select("name","salary").show()
//    df.filter(col("salary")>60000).select("*").show()
//    df.filter(col("city")==="Delhi" && col("dept")==="IT").show()
//    df.select(col("salary")*0.1).alias("bonus").show()
//  df.withColumn("country",lit("India")).show()
//df.drop(col("country")).show()
//    df.withColumnRenamed("salary","emp_salary").show()
//df.count()
//    df.select(col("dept")).distinct().show()


//    Find department with lowest salary.
    // Find top 2 highest-paid employees in each department.
    // Find total salary per city.
    // Find average age per department.
    // Find employee with maximum salary (global).
    // Find second highest salary overall.
    // Find median salary per department (harder).

//
//df.groupBy(col("dept")).agg(min(col("salary"))).select("*").show()
////   val df= df.groupBy("dept").agg(max(col("salary"))).limit(2).show()
//
//    df.groupBy(col("city")).agg(sum(col("salary"))).show()
//      df.agg(max("salary")).show()
//       val max_salary= df.agg(max("salary")).select("*").show().
//    df.filter(col("salary")>max_salary).show()?

val dept_df=spark.read.option("header",true).option("inferSchema",true).
  csv("C:\\Users\\saksh\\IdeaProjects\\Spark_try\\src\\main\\scala\\Input_file\\dept.csv")
    emp_df.show()
    dept_df.show()
/**Inner join employees with departments on dept.
 Left join employees with departments.
 Right join employees with departments.
 Full outer join employees with departments.
 Cross join employees with departments.
 Show employees with no matching department.

//**/
//   val df_1=emp_df.join(dept_df,Seq("dept"),"inner")
//    df_1.show()
//     val df_2=emp_df.join(dept_df,Seq("dept"),"left")
//    df_2.show()
//    val df_3=emp_df.join(dept_df,Seq("dept"),"full_outer")
//    df_3.show()
//
//    val df_4=emp_df.join(dept_df,Seq("dept"),"cross")
//    df_4.show()
//
//    val df_5=emp_df.join(dept_df,Seq("dept"),"left_anti")
//    df_5.show()

 Show departments with no employees.
 Join and show employee name + dept_name + manager.
 Perform broadcast join between employees and departments.
 Find employees managed by Rahul.
 Count employees per department using join.
 List employees in Finance department.
 Show employees with dept_name instead of dept code.
 Perform self-join on employees (find colleagues from same dept).
 Perform semi join (only employees present in department list).
**/
//    val df_1=emp_df.join(dept_df,Seq("dept"),"inner").select("dept","name","manager")
//    df_1.show()
//    val df_2=emp_df.join(dept_df,Seq("dept"),"inner")
////    df_2.show()
//
//    val df_3=emp_df.join(dept_df,Seq("dept"),"inner").filter(col("manager")==="Rahul")
//    df_3.show()
//    val df_4=emp_df.join(dept_df,Seq("dept"),"inner").groupBy("dept").agg(count("*"))
//    df_4.show()


//    val df_self = emp_df.as("e1").join(emp_df.as("e2"), ($"e1.dept" === $"e2.dept") && ($"e1.dept_")
//
//emp_df.repartition(5)
//    println(emp_df.rdd.getNumPartitions)
//
//    val emp_1=emp_df.coalesce(2)
//    println(emp_1.rdd.getNumPartitions)
//
//    emp_df.persist(StorageLevel.MEMORY_ONLY)

val df_sales=spark.read.option("header",true)
  .csv("C:\\Users\\saksh\\IdeaProjects\\Spark_try\\src\\main\\scala\\Input_file\\sales_prac.csv")
    df_sales.show()

//    df_sales.groupBy(month(col("date"))).agg(sum(col("revenue"))).show()
//    df_sales.groupBy("date").pivot("product").agg(sum("revenue")).show()

//    Find top-selling product each month.
//    Calculate cumulative revenue per product using window functions.
//    Rank products by revenue each month.
//    Find products contributing more than 40% revenue per month.
//      Calculate moving average of revenue.
//      Find month with max revenue overall.
//      Find month-over-month growth % of revenue.
//      Explode array column into multiple rows.


//val window_highest=Window.partitionBy(month(col("date"))).orderBy(col("revenue").desc)
//
//  val df_top=df_sales.withColumn("rank",row_number().over(window_highest))
//    .filter(col("rank")===1)
//    .select("product","date","revenue")
//    df_top.show()

//  val window_cum=Window.partitionBy("product").orderBy(col("date").desc)
//    .rowsBetween(Window.unboundedPreceding,Window.currentRow)
//
//    val df_cum=df_sales.withColumn("cum_sum",sum("revenue").over(window_cum))
//    df_cum.show()
//
//    val window_rank=Window.partitionBy(month(col("date"))).orderBy(col("revenue").desc)
//      val df_rank=df_sales.withColumn("rank",rank().over(window_rank))
//    df_rank.show()
//    //      Find month with max revenue overall.

//    df_sales.groupBy(month(col("date"))).agg(max(col("revenue"))).show()

//    Find employees who earn more than average salary of their department.
//      Find employees who earn the maximum salary in their city.
//    Find employees having same salary in same department.
//    Find employees who do not belong to any department.

//    val avg_sal=Window.partitionBy(col("dept"))
//  val result=emp_df.withColumn("avg",avg(col("salary")).over(avg_sal))
//    val df=result.filter(col("salary")>col("avg"))
//    df.show()
//    val window_max=Window.partitionBy("city")
//    val max_window=emp_df.withColumn("max",max(col("salary")).over(window_max))
//      val res=max_window.filter(col("salary")===col("max"))
//    res.show()

//  val salry_dept=Window.partitionBy("dept","salary")
//    val sal=emp_df.withColumn("salry",count("*").over(salry_dept))
//    val res=sal.filter(col("salry")>1)
//    res.show()
//
//val high=Window.partitionBy("dept").orderBy(col("salary").desc)
//    val high_sal=emp_df.withColumn("rank",dense_rank().over(high))
//    val high_emp=high_sal.filter(col("rank")===1).drop("rank")
//    high_emp.show()


//  emp_df.write.orc("C:\\Users\\saksh\\IdeaProjects\\Spark_try\\src\\main\\scala\\Input_file\\emp_orc12")

//
//emp_df.write.partitionBy("dept").csv("C:\\Users\\saksh\\IdeaProjects\\Spark_try\\src\\main\\scala\\Input_file\\dept.csv12")
//
//emp_df.write.option("compression","gzip")
//
//    Find 2nd highest salary per department.
//    Find employees with salary greater than their department average.

//    val nd_high=Window.partitionBy("dept").orderBy(col("salary").desc)
//    val high=emp_df.withColumn("rank",dense_rank().over(nd_high))
//    val highest_sal=high.filter(col("rank")===2&&3)
//    highest_sal.show()
//
//    val dept_avg=Window.partitionBy("dept")
//    val avg_sal=emp_df.withColumn("avg_sal",avg("salary").over(dept_avg))
//    val result=avg_sal.filter(col("salary")>col("avg_sal"))
//    result.show()

//    val input_rdd=sc.textFile("C:\\Users\\saksh\\IdeaProjects\\Spark_try\\src\\main\\scala\\Input_file\\wordcount_1")
//   //split lines into word
//    val word_rdd=input_rdd.flatMap(line => line.split(" "))
//    // map each word to (word,1)
//    val wordpairrdd=word_rdd.map(word => (word,1))
//    //reducebykey for word count
//    val word_count_rdd=wordpairrdd.reduceByKey(_+_)
//    word_count_rdd.collect().foreach(println)
//    spark.stop


//    Dataset: Combination of employees & sales
//// Find employees who earn more than average salary of their department.
//    val df_avg=emp_df.groupBy(col("dept")).agg(avg("salary").alias("avg_salary"))
//    df_avg.filter(col("salary")>col("avg_salary")).show()

//    val window_avg=Window.partitionBy("dept")
//    val df_avg_dept=emp_df.withColumn("avg_sal",avg("salary").over(window_avg))
//    val result=df_avg_dept.filter(col("salary")>col("avg_sal")).drop("avg_sal")
//    result.show()


// Find employees who earn the maximum salary in their city.
//    val emp_max_sal=Window.partitionBy(col("city"))
//    val df_max=emp_df.withColumn("max_salary",max("salary").over(emp_max_sal))
//    val result1=df_max.filter(col("salary")>col("max_salary"))
//    result1.show()

//// Find employees having same salary in same department.
//    val df_same=Window.partitionBy("dept").orderBy(col("salary").desc)
//    val same_salary=emp_df.withColumn("rank",dense_rank().over(df_same))
//    val result2=same_salary.filter(col("rank")==="salary")
//    result2.show()
// Find employees who do not belong to any department.
// Find 2nd highest salary per department.
    val windoe_dept=Window.partitionBy("dept").orderBy(col("salary").desc)
    val df_rank=emp_df.withColumn("rank",dense_rank().over(windoe_dept))
    val result5=df_rank.filter(col("rank")===2)
    result5.show()
// Find employees with salary greater than their department average.
// Find employees who joined after 2020 (using date column).
// Find duplicates in employees DataFrame.
// Remove duplicates keeping highest salary.
// Combine employee & sales dataset → match employees with sales by city.
// Find cities where average employee salary > company average.
// Find department with youngest average employee age.
//    val window_sal=Window.partitionBy("dept")
//    val df_age=emp_df.withColumn("avg_age",avg("age").over(window_sal))
//    val result3=
// Create hierarchy (manager → employees) using self-join.
// Write employees grouped by dept into separate partitioned Parquet files.
// Write sales grouped by month into Hive table.


val rdd=sc.textFile("C:\\Users\\saksh\\IdeaProjects\\Spark_try\\src\\main\\scala\\Input_file\\wordcount_1")

   val split= rdd.flatMap(line=>line.split(" "))
    val word_count=split.map(word=>(word,1))
    val reslut_count=word_count.reduceByKey(_+_)
    reslut_count.collect().foreach(println)
    spark.stop()











  }

}

package Spark_Assignment_4

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, min}
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructField, StructType}

object sales_data_analysis {
  def main(args: Array[String]): Unit = {


    val Spark = SparkSession.builder().master("local").appName("read_csv").getOrCreate()

    val sc = Spark.sparkContext
    sc.setLogLevel("error")

    val df1 = Spark.read.option("header", true).option("InferSchema", true).
      csv("C:\\Users\\saksh\\IdeaProjects\\Spark_try\\src\\main\\scala\\Input_file\\sales_data_analysis")
    df1.show()
    df1.printSchema()
//
//    val sale=StructType(Array(StructField("sale_id",IntegerType,true),
//      StructField("sale_date",DateType,true),
//      StructField("product_name",StringType,true),
//      StructField("unit_price",DoubleType,true),
//      StructField("total_amount",DoubleType,true),
//      StructField("region",StringType,true),
//      StructField("salesperson",StringType,true),
//      StructField("status",StringType,true),
//    ))
//
//    val df=Spark.read.option("header",true).option("header",true).schema("sale").
//      csv("C:\\Users\\saksh\\IdeaProjects\\Spark_try\\src\\main\\scala\\Input_file\\sales_data_analysis")
//      df.show()
//      df.printSchema()
//    1. Select all columns from the `sales` table.
    df1.select("*").show()
//    2. Select the `sale_id`, `product_name`, and `total_amount` of all completed sales.
    df1.select("sale_id","product_name","total_amount").show()
//    3. Select the `product_name` and `total_amount` of all sales where the `total_amount` is greater than 1000.
    df1.filter(col("total_amount")>1000).select("product_name","total_amount").show()
//    4. Select the sales where the `region` is 'North'.
    df1.filter(col("region")==="North").show()
//    5. Select the `sale_id` and `quantity` of all sales where the `quantity` is less than 10.
    df1.filter(col("quantity")<10).show()

////    90. Select the `salesperson` with the lowest total sales and display it as 'Lowest Salesperson'.
//    val df2=df1.agg(min(col("total_amount").alias("lowest_salesperson"))).show()
//    df1.select("salesperson",df1).sh

//    91. Find the total `total_amount` of sales for each `product_name` where the total is greater than 2000.
//    92. Find the average `unit_price` of sales for each `region` where the average is less than 100.
//    93. Count the number of `Cancelled` sales for each `region` where the count is greater than 2.
//    94. Find the maximum `quantity` of sales for each `product_name` where the maximum is less than 20.
//    95. Find the minimum `unit_price` of sales where the `status` is 'Completed' and display it as 'Min Completed Price'.
//    96. Select the `region` where the average `total_amount` of sales is highest and display it as 'Top Region for Sales'.
//    97. Find the total `total_amount` of sales for each `salesperson` where the total is greater than 1500.
//    98. Find the average `total_amount` of sales where the `status` is 'Cancelled' and display it as 'Average Cancelled Amount'.
//    99. Count the number of sales where the `unit_price` is between 100 and 200 and display it as 'Total Sales in Range'.
//    100. Find the maximum `total_amount` of sales where the `product_name` is 'Smartwatch' and display it as 'Max Smartwatch Sale'.
//



  }
}

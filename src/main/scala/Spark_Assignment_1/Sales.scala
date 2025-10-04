package Spark_Assignment_1

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._

object Sales {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().master("local").appName("read_csv").getOrCreate()
    val sc= spark.sparkContext
    sc.setLogLevel("error")
val df=spark.read.option("header",true).option("inferSchema",true).
  csv("C:\\Users\\saksh\\IdeaProjects\\Spark_try\\src\\main\\scala\\Input_file\\Sales.csv")
df.printSchema()
    df.show()

 val sales=StructType(Array(StructField("sale_id",IntegerType,true),
   StructField("customer_name",StringType,true),
   StructField("sale_date",DateType,true),
   StructField("total_amount",DecimalType(10,2),true),
   StructField("city",StringType,true),
   StructField("payment_status",StringType,true)))

    val df1=spark.read.option("header",true).option("InferSchema",true).schema(sales).
      csv("C:\\Users\\saksh\\IdeaProjects\\Spark_try\\src\\main\\scala\\Input_file\\Sales.csv")
    df1.printSchema()
    df1.show()

    println("1.Select the sale_id and customer_name for all sales that were paid.")
    df1.filter(col("payment_status")==="Paid").select("sale_id","customer_name").show()

    println("2.Select the customer_name, sale_date, and total_amount for sales greater than 200.")
    df1.filter(col("total_amount")>200).select("customer_name", "sale_date", "total_amount").show()

    println("3.Select all columns for sales that occurred on '2024-11-25'.")
df1.filter(col("sale_date")==="2024-11-25").show()

    println("4.Select the customer_name, sale_date, and payment_status for sales where the payment_status is 'Paid'.")
    df1.filter(col("payment_status")==="Paid").select("customer_name", "sale_date", "payment_status").show()

println("5.Select the customer_name and city for customers who made purchases in 'Pune'.")
    df1.filter(col("city")==="Pune").select("customer_name","city").show()

println("6.Select the customer_name and total_amount for sales with a total amount between 100 and 300")
    df1.filter(col("total_amount")between (100, 300)).select("customer_name","total_amount").show()

    println("7.Select all columns for sales with a total_amount less than 150.")
    df1.filter(col("total_amount")<150).show()

println("8.Select distinct payment_status from the sales table.")
    df1.select("payment_status").distinct().show()

    println("9.Select the customer_name, sale_date, and payment_status for sales where the customer’s name starts with 'S'.")
    df1.filter(col("customer_name").like("S%")).select("customer_name","sale_date","payment_status").show()

    println("10.Select the customer_name, sale_date, and city where the city is either 'Mumbai' or 'Nagpur'.")
    df1.filter(col("city")==="Nagpur" || col("city")==="Mumbai").select("customer_name","sale_date","city").show()

println("11.Select the customer_name and total_amount for sales where the total_amount is greater than 100 and payment_status is 'Paid'.")
    df1.filter(col("payment_status")==="Paid" && col("total_amount")>100).select("customer_name", "total_amount").show()

println("12.Select all columns from the sales table for sales where the total_amount is less than 200 and the payment_status is 'Pending'.")
    df1.filter(col("payment_status")==="Pending" && col("total_amount")<100).show()

println("13.Select the customer_name and sale_date for sales that took place before '2024-11-20'.")
    df1.filter(col("sale_date")<"2024-11-20").select("customer_name", "sale_date").show()

    println("14.Select the customer_name, sale_date, and payment_status for sales where the payment_status is not 'Pending'.")
    df1.filter(col("payment_status")=!="Pending").select("customer_name", "sale_date", "payment_status").show()

println("15.Select the customer_name and city for customers from 'Mumbai' with a total amount greater than 100.")
    df1.filter(col("city")==="Mumbai" && col("total_amount")>100).select("customer_name", "city").show()

println("16.Select all columns for sales where the sale_date is between '2024-11-18' and '2024-11-25'.")
    df1.filter(col("sale_date")between ("2024-11-18", "2024-11-25")).show()

    println("17.Select the customer_name and total_amount for sales that were made in the city 'Pune' and have a total amount less than 500.")
    df1.filter(col("city")==="Pune" && col("total_amount")<500).select("customer_name", "total_amount", "payment_status").show()

    println("18.Select the customer_name and sale_date for sales that occurred on or after '2024-11-19'")
    df1.filter(col("sale_date")>"2024-11-29").select("customer_name", "sale_date").show()

println("19.Select the customer_name and total_amount where the total_amount is greater than 100 and less than 500.")
    df1.filter(col("total_amount")>100 && col("total_amount")<500).select("customer_name", "total_amount").show()

println("20.Select the customer_name and payment_status for sales where the payment_status is 'Pending' and the city is 'Nagpur'.")
    df1.filter(col("city")==="Nagpur" && col("payment_status")==="Pending").select("customer_name", "payment_status").show()

    println("21.Select the customer_name, sale_date, and total_amount for sales made in 'Mumbai' and with a total amount greater than 150.")
    df1.filter(col("city")==="Mumbai" && col("total_amount")>150).select("customer_name", "total_amount", "sale_date").show()

println("22.Select all columns for sales where the sale_date is '2024-11-20' and payment_status is 'Paid'.")
    df1.filter(col("sale_date")==="2024-11-20" && col("payment_status")==="Paid").show()

println("23.Select the customer_name and city for customers from 'Pune' where the payment_status is 'Pending'.")
    df1.filter(col("city")==="Pune" && col("payment_status")==="Pending").select("customer_name", "city").show()

println("24.Select the customer_name and total_amount for sales that are either in 'Pune' or 'Nagpur' with a total amount greater than 300.")
    df1.filter(col("city")==="Nagpur" || col("city")==="Pune" && col("total_amount")>300).select("customer_name","total_amount").show()

println("25.Select the customer_name, sale_date, and total_amount for sales where the sale_date is before '2024-11-22'.")
    df1.filter(col("sale_date")<"2024-11-22").select("customer_name", "sale_date","total_amount").show()

println("26.Select all columns for sales where the payment_status is 'Paid' and the city is 'Mumbai'.")
    df1.filter(col("payment_status")==="Paid" && col("city")==="Mumbai").show()

    println("27.Select the customer_name and payment_status for sales where the payment_status is 'Pending' " +
      "and the total_amount is greater than 200.")
    df1.filter(col("payment_status")==="Pending" && col("total_amount")>200).select("customer_name","payment_status").show()

println("28.Select the customer_name, sale_date, and total_amount for sales that are from 'Nagpur' and have a total amount less than 150.")
    df1.filter(col("city")==="Nagpur" && col("total_amount")<150).select("customer_name", "total_amount", "sale_date").show()

println("29.Select the customer_name and sale_date for sales where the sale_date is on or after '2024-11-18'.")
    df1.filter(col("sale_date")>"2024-11-18").select("customer_name", "sale_date").select("customer_name","sale_date").show()

println("30.Select the customer_name and total_amount where the total_amount is greater than 150 and the city is 'Mumbai'.")
    df1.filter(col("city")==="Mumbai" && col("total_amount")>150).select("customer_name", "total_amount").show()

    println("31.Select the customer_name and total_amount for sales where the payment_status is 'Paid' and sale_date is after '2024-11-19'.")
    df1.filter(col("sale_date")>"2024-11-19" && col("payment_status")==="Paid").select("customer_name", "total_amount").show()

println("32.Select the customer_name and sale_date for sales where the total_amount is between 50 and 150 and the payment_status is 'Pending'.")
    df1.filter(col("total_amount").between (50,150) && col("payment_status")==="Pending") .select("customer_name","sale_date").show()

    println("33.Select the customer_name, sale_date, and payment_status for sales where the payment_status is 'Paid' and " +
      "the sale_date is between '2024-11-20' and '2024-11-25'.")
    df1.filter(col("payment_status")==="Paid" && col("sale_date").between("2024-11-20","2024-11-25")).
      select("customer_name","sale_date","Payment_status").show()

println("34.Select the customer_name and city where the city is 'Mumbai' or 'Nagpur' and the payment_status is 'Pending'.")
    df1.filter(col("payment_status")==="Pending" && col("city")==="Mumbai" || col("city")==="Nagpur").select("customer_name","city").show()

println("35.Select the customer_name and sale_date where the sale_date is between '2024-11-18' and " +
  "'2024-11-25' and the total_amount is greater than 100.")
    df1.filter(col("total_amount")>100 && col("sale_date").between("2024-11-18","2024-11-25")).
      select("customer_name","sale_date").show()

println("36.Select the customer_name and total_amount for sales where the total_amount is between " +
  "200 and 500 and the payment_status is 'Paid'.")
    df1.filter(col("total_amount").between (200, 500) && col("payment_status")==="Paid") .select("customer_name","total_amount").show()

println("37.Select the customer_name and payment_status for sales where the payment_status is 'Pending' and the city is 'Nagpur'.")
    df1.filter(col("payment_status")==="Pending" && col("city")==="Nagpur").select("customer_name","payment_status").show()

println("38.Select the customer_name, sale_date, and payment_status for sales where the sale_date is before '2024-11-20' and " +
  "the payment_status is 'Paid'.")
    df1.filter(col("sale_date")<"2024-11-20" && col("payment_status")==="Paid").select("customer_name", "sale_date","payment_status").show()

println("39.Select all columns for sales where the total_amount is greater than 100 but less than 500 and payment_status is 'Paid'.")
    df1.filter(col("total_amount")>100 && col("total_amount")<500 && col("payment_status")==="Paid") .show()

    println("40.Select the customer_name, sale_date, and city for sales made in 'Mumbai' and 'Pune'.")
    df1.filter(col("city")==="Mumbai" && col("city")==="Pune").select("customer_name","sale_date","city").show()

    println("41.Select the customer_name, sale_date, and total_amount for sales where the total_amount is greater than 100 and the city is 'Nagpur'.")
    df1.filter(col("city")==="Nagpur" && col("total_amount")>100).select("customer_name", "sale_date","total_amount").show()

println("42.Select the customer_name, total_amount, and payment_status where the payment_status is 'Paid' and city is 'Pune'.")
    df1.filter(col("payment_status")==="Paid" && col("city")==="Pune").select("customer_name","total_amount","payment_status").show()

println("43.Select all columns for sales made before '2024-11-20' with a payment_status of 'Paid'.")
    df1.filter(col("sale_date")<"2024-11-20" && col("payment_status")==="Paid").show()

println("44.Select the customer_name and sale_date where the sale_date is after '2024-11-19' and the total_amount is less than 500.")
    df1.filter(col("sale_date")>"2024-11-19" && col("total_amount")<500).select("customer_name", "sale_date").show()

println("45.Select the customer_name and city for sales where the city is either 'Pune' or 'Mumbai'.")
    df1.filter(col("city")==="Mumbai" || col("city")==="Pune").select("customer_name","city").show()

    println("46.Select the customer_name and total_amount for sales made in 'Nagpur' with a total_amount less than 100.")
    df1.filter(col("city")==="Nagpur" || col("total_amount")<100).select("customer_name","total_amount").show()

println("47.Select the customer_name and sale_date for sales where the sale_date is before '2024-11-21' and the payment_status is 'Paid'.")
    df1.filter(col("sale_date")<"2024-11-21" && col("payment_status")==="Paid").select("customer_name","sale_date").show()

println("48.Select the customer_name, total_amount, and city for sales where the city is 'Pune' and the payment_status is 'Pending'.")
    df1.filter(col("payment_status")==="Pending" && col("city")==="Pune").select("customer_name","total_amount","city").show()

println("49.Select the customer_name and payment_status for sales where the payment_status is 'Paid' and the total_amount is greater than 150.")
    df1.filter(col("total_amount")>150 && col("payment_status")==="Paid") .select("customer_name","payment_status").show()

    println("50.Select the customer_name, total_amount, and payment_status for sales made on or after '2024-11-19' and with a total_amount less than 500.")
    df1.filter(col("sale_date")>"2024-11-19" && col("total_amount")<500).select("customer_name","total_amount","payment_status").show()


  }
}

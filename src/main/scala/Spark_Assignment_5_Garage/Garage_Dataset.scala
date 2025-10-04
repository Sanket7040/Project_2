package Spark_Assignment_5_Garage

import org.apache.avro.LogicalTypes.date
import org.apache.spark.sql.catalyst.ScalaReflection.universe.show
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.catalyst.expressions.aggregate.Max
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{aggregate, coalesce, col, cos, count, datediff, dense_rank, desc, lit, max, min, rank, row_number, sum, when}
import org.apache.spark.sql.types.{DateType, DecimalType, DoubleType, IntegerType, StringType, StructField, StructType}

import scala.Console.println

object Garage_Dataset {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName("read_csv").getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("error")

    val customer = StructType(Array(
      StructField("CID", IntegerType),
      StructField("CName", StringType),
      StructField("CAdd", StringType),
      StructField("C_Contact", IntegerType),
      StructField("C_CreditDays", IntegerType),
      StructField("CJ_Date", DateType),
      StructField("SEX", StringType)
    ))

    val df_cust = spark.read.option("header", true).option("inferSchema", true).schema(customer).
      csv("C:\\Users\\saksh\\IdeaProjects\\Spark_try\\src\\main\\scala\\Garage_Dataset\\Customer.csv")
    println("Customer_table")
    df_cust.show()
    //df_cust.printSchema()


    val employee = StructType(Array(
      StructField("EID", IntegerType),
      StructField("EName", StringType),
      StructField("EJob", StringType),
      StructField("EAdd", StringType),
      StructField("E_Contact", IntegerType),
      StructField("E_Sal", IntegerType),
      StructField("E_Doj", DateType),
      StructField("E_Dol", DateType)
    ))
    val df_emp = spark.read.option("header", true).option("InferSchema", true).schema(employee).
      csv("C:\\Users\\saksh\\IdeaProjects\\Spark_try\\src\\main\\scala\\Garage_Dataset\\employee.csv")
    println("employee_table")
    df_emp.show()
    //    df_emp.printSchema()

    val purchase = StructType(Array(
      StructField("PID", IntegerType),
      StructField("VID", IntegerType),
      StructField("SPID", IntegerType),
      StructField("PQty", IntegerType),
      StructField("SPRate", IntegerType),
      StructField("SPGST", DecimalType(10, 2)),
      StructField("PDate", DateType),
      StructField("Transcost", IntegerType),
      StructField("Total", DecimalType(10, 2)),
      StructField("RCV_EID", IntegerType),
    ))

    val df_pur = spark.read.option("header", true).option("InferSchema", true).schema(purchase).
      csv("C:\\Users\\saksh\\IdeaProjects\\Spark_try\\src\\main\\scala\\Garage_Dataset\\Purchase.csv")
    println("Purchase_table")
    df_pur.show()
    //    df_pur.printSchema()

    val service = StructType(Array(
      StructField("SID", IntegerType),
      StructField("CID", IntegerType),
      StructField("EID", IntegerType),
      StructField("SPID", IntegerType),
      StructField("TYP_VEH", StringType),
      StructField("Veh_No", StringType),
      StructField("Typ_Ser", StringType),
      StructField("Ser_Date", DateType),
      StructField("Qty", DecimalType(10, 2)),
      StructField("Sp_Rate", IntegerType),
      StructField("Sp_Amt", IntegerType),
      StructField("Sp_G", IntegerType),
      StructField("Ser_Amt", IntegerType),
      StructField("Comm", IntegerType),
      StructField("Total", IntegerType),
    ))

    val df_ser = spark.read.option("header", true).option("InferSchema", true).schema(service).
      csv("C:\\Users\\saksh\\IdeaProjects\\Spark_try\\src\\main\\scala\\Garage_Dataset\\Service.csv")
    println("Service_table")
    df_ser.show()
    //    df_ser.printSchema()

    val spareparts = StructType(Array(
      StructField("SPID", IntegerType),
      StructField("SPName", StringType),
      StructField("SPRate", IntegerType),
      StructField("SPUnit", StringType),
    ))

    val df_spare = spark.read.option("header", true).option("InferSchema", true).schema(spareparts).
      csv("C:\\Users\\saksh\\IdeaProjects\\Spark_try\\src\\main\\scala\\Garage_Dataset\\spareparts.csv")
    println("spareparts_table")
    df_spare.show(truncate = false)
    //    df_spare.printSchema()

    val vendors = StructType(Array(
      StructField("VID", IntegerType),
      StructField("VName", StringType),
      StructField("VAdd", StringType),
      StructField("VContact", IntegerType),
      StructField("VCreditDays", IntegerType),
      StructField("VJ_Date", DateType),
    ))

    val df_vendor = spark.read.option("header", true).option("InferSchema", true).schema(vendors).
      csv("C:\\Users\\saksh\\IdeaProjects\\Spark_try\\src\\main\\scala\\Garage_Dataset\\Vendors.csv")
    println("Vendor_table")
    df_vendor.show()
    //df_vendor.printSchema()



    println("Q.1  List all the customers serviced.")
    //select * from customers c inner join service s on c.CID=S.CID
    val df1=df_cust.join(df_ser,Seq("CID"),"inner").select(df_cust("*"))
    df1.show()

    println("Q.2  Customers who are not serviced.")
   val df2=df_cust.join(df_ser,Seq("CID"),"left").filter(df_ser("CID").isNull).select(df_cust("*"))
   df2.show()

    println("Q.3  Employees who have not received the commission.")
  val df3=df_emp.join(df_ser,Seq("EID"),"left").filter(df_ser("EID").isNull).select(df_emp("EName"))
    df3.show()

    println("Q.4  Name the employee who have maximum Commission.")
  val max_comm=df_emp.join(df_ser,Seq("EID"),"inner").agg(max("Comm").alias("max_comm")).collect()(0).getAs[0]("max_comm")

    val df4=df_emp.join(df_ser,Seq("EID"),"inner").filter(col("Comm")===max_comm).select("EName","Comm").limit(1)
    df4.show()


    println("Q.5  Show employee name and minimum commission amount received by an employee.")
    val min_comm= df_emp.join(df_ser,Seq("EID"),"inner").agg(min("Comm").alias("min_comm")).collect()(0).getAs[Int]("min_comm")

    val df5=df_emp.join(df_ser,Seq("EID"),"inner").filter(col("Comm")===min_comm).select("EName","Comm")
    df5.show()


    println("Q.6  Display the Middle record from any table.")
    val rowcount=df_emp.count()
    val middleindex=(rowcount/2).toInt

    val middlerecord=df_emp.orderBy("EID")
      .limit(middleindex+1)
      .orderBy(desc("EID"))
      .limit(1)

    middlerecord.show()

    println("Q.7  Display last 4 records of any table.")

    val lastrecord=df_emp.orderBy(desc("EID"))
      .limit(4)
       lastrecord.show()

    println("Q.8  Count the number of records without count function from any table.")
    df_spare.agg(sum(lit(1).alias("count_no"))).show()

    println("Q.9  Delete duplicate records from \"Ser_det\" table on cid.(note Please rollback after execution).")



    println("Q.10 Show the name of Customer who have paid maximum amount ")
    val max_amt=df_cust.join(df_ser,Seq("CID"),"inner").agg(max(col("total")).alias("max_amt")).collect()(0).getAs[Int]("max_amt")

    val df10=df_cust.join(df_ser,Seq("CID"),"inner").filter(col("total")===max_amt).select("CName","total")
    df10.show()



    println("Q.11 Display Employees who are not currently working.")
    val df11=df_emp.join(df_ser,Seq("EID"),"left").filter(df_ser("EID").isNull).select("EName","EID").show()


    println("Q.12 How many customers serviced their two wheelers.")
    val count_twowheeler=df_cust.join(df_ser,Seq("CID"),"left").filter(col("TYP_VEH")==="Two Wheeler").agg(count(col("*")).alias("count"))
    count_twowheeler.show()

    println("Q.13 List the Purchased Items which are used for Customer Service with Unit of that Item.")
   val df_purchase_item=df_pur.join(df_ser,Seq("SPID"),"inner").join(df_spare,Seq("SPID"),"inner").select("SPName","Qty")

    df_purchase_item.show()

    println("Q.14 Customers who have Colored their vehicles.")
    val df14=df_cust.join(df_ser,Seq("CID"),"inner").filter(col("Typ_Ser")==="Color")
    df14.show()

    println("Q.15 Find the annual income of each employee inclusive of Commission")
    val df15=df_emp.join(df_ser,Seq("EID"),"left").
      withColumn("comm_clean",coalesce(col("Comm"),lit(0)))
      .withColumn("annual_income",(col("E_Sal")*12)+(col("Comm_clean")))
      .drop("Comm_clean")
      .dropDuplicates("EID")
    df15.select("EName","annual_income").show()



    println("Q.16 Vendor Names who provides the engine oil.")
    val df16=df_pur.join(df_spare,Seq("SPID"),"inner")
      .join(df_vendor,Seq("VID"),"inner")
      .filter(col("SPName")==="Two Wheeler Engine Oil" || col("SPName")==="Four Wheeler Engine Oil")
      .select("VID","VName")

    df16.show()

    println("Q.17 Total Cost to purchase the Color and name the color purchased.")
    val total_pur=df_spare.join(df_pur,Seq("SPID"),"left").
      filter(col("SPName")===("White Colour" ) || col("SPName")===("Black Colour"))
      .groupBy("SPName")
      .agg(sum(col("total")).alias("total_cost"))
       total_pur.show()




    println("Q.18 Purchased Items which are not used in \"Ser_det\".")
    val pur_notused=df_pur.join(df_ser,Seq("SPID"),"left").
      join(df_spare,Seq("SPID"),"inner")
      .filter(df_ser("SPID").isNull).select("SPName")

    pur_notused.show()


    println("Q.19 Spare Parts Not Purchased but existing in Sparepart")
   val spare_pur=df_spare.join(df_pur,Seq("SPID"),"left")
     .filter(df_pur("SPID").isNull).select("SPName","SPID")
    spare_pur.show(truncate = false)

    println("Q.20 Calculate the Profit/Loss of the Firm. Consider one month salary of each employee for Calculation.")
//  val ser_amt=df_ser.agg(sum(col("total")).alias("total_amt_ser")).collect()(0).getAs[Double]("total_amt_ser")
//
//    val pu_amt=df_pur.agg(sum(col("total")).alias("total_pur")).collect()(0).getAs[Double]("total_pur")
//
//    val emp_total=df_emp.agg(sum(col("E_Sal")).alias("total_sal")).collect()(0).getAs[Double]("total_sal")
//
//    val profit_loss=ser_amt-(pu_amt+emp_total)
//
//    println(s"the total profit/loss of firm is: $profit_loss")

    println("Q.21 Specify the names of customers who have serviced their vehicles more than one time.")

    val cust_twice=df_cust.join(df_ser,Seq("CID"),"inner").groupBy("CName").agg(count("*").alias("ser_count")).filter(col("ser_count")>1).select("CName")

    cust_twice.show()



    println("Q.22 List the Items purchased from vendors locationwise.")
    val ven_name=df_pur.join(df_vendor,Seq("VID"),"inner").join(df_spare,Seq("SPID"),"inner").select("SPName")
    ven_name.show()


    println("Q.23 Display count of two wheeler and four wheeler from ser_details")
    val count_veh=df_ser.groupBy("TYP_VEH").agg(count(col("*")).alias("veh_count"))
    count_veh.show()


    println("Q24 Display name of customers who paid highest SPGST and for which item ")
    val cust_gst=df_cust.join(df_ser,Seq("CID"),"inner").join(df_spare,Seq("SPID")).agg(max(col("Sp_G")).alias("max_gst")).collect()(0).getAs[Int]("max_gst")

    val df24=df_cust.join(df_ser,Seq("CID"),"inner").join(df_spare,Seq("SPID"),"inner").filter(col("Sp_G")===cust_gst).select("CName","SPName")
     df24.show()

    println("Q25 Display vendors name who have charged highest SPGST rate  for which item")
    val max_gst=df_vendor.join(df_pur,Seq("VID"),"inner")
      .join(df_spare,Seq("SPID"),"inner")
      .agg(max(col("SPGST")).alias("max_GST"))
      .first().getAs[java.math.BigDecimal]("max_GST")

    val df25=df_vendor.join(df_pur,Seq("VID"),"inner").join(df_spare,Seq("SPID"),"inner")
      .filter(col("SPGST")===max_gst)
      .select("VName","SPName")
    df25.show()

    println("Q26   list name of item and employee name who have received item ")

    val df26=df_ser.join(df_emp,Seq("EID"),"inner").join(df_spare,Seq("SPID"),"inner").select("SPName","EName")
    df26.show()

    println(" Q27 Display the Name and Vehicle Number of Customer who serviced his vehicle, " +
      "And Name the Item used for Service, And specify the purchase date of that Item with his vendor " +
      "and Item Unit and Location, And employee Name who serviced the vehicle. for Vehicle NUMBER \"MH-14PA335\".'")

//    val df27=df_cust.join(df_ser,Seq("CID"),"inner").join(df_spare,Seq("SPID")).




    println("Q28 who belong this vehicle  MH-14PA335\" Display the customer name ")

    val df28=df_cust.join(df_ser,Seq("CID"),"inner").filter(col("Veh_No")==="MH14PA335").select("CName")
    df28.show()


    println("Q29 Display the name of customer who belongs to New York and when he /she service their  vehicle on which date    ")

    val df29=df_cust.join(df_ser,Seq("CID"),"inner")
             .filter(col("CAdd")==="New York")
             .select("CName","Ser_Date")
     df29.show()

    println("Q 30 from whom we have purchased items having maximum cost?")
    val max_cost=df_pur.join(df_vendor,Seq("VID"),"inner")
      .agg(max("Total").alias("max_cost")).first().getAs[java.math.BigDecimal]("max_cost")

    val df30=df_pur.join(df_vendor,Seq("VID"),"inner").filter(col("Total")===max_cost)
      .select("VName")
    df30.show()

    println("Q31 Display the names of employees who are not working as Mechanic and that employee done services   .")
    val df31=df_emp.join(df_ser,Seq("EID"),"inner").filter(col("EJob")=!="Mechanic").select("EName")

    df31.show()


    println("Q32 Display the various jobs along with total number of employees in each job. The output should\ncontain " +
      "only those jobs with more than two employees.")

    val df32=df_emp.join(df_ser,Seq("EID"),"inner").groupBy("EJob").agg(count(col("*")).alias("job_count"))
      .filter(col("job_count")>2)
    df32.show()



    println("Q33 Display the details of employees who done service  and give them rank according to their no. of services .")
    val df33_count=df_emp.join(df_ser,Seq("EID"),"inner")
      .groupBy("EID","EName")
      .agg(count(col("*")).alias("ser_count"))

      val window_f=Window.orderBy(col("ser_count").desc)

        val df33_rank=df33_count.withColumn("rank",dense_rank().over(window_f))

    df33_rank.show()


    println("Q 34 Display those employees who are working as Painter and fitter and who provide service and total count of " +
      "service done by fitter and painter  ")
    val df34=df_emp.join(df_ser,Seq("EID"),"inner")
      .filter(col("EJob")==="Painter" || col("EJob")==="Fitter")
      .groupBy("EJob","EName")
      .agg(count(col("*")).alias("ser_count"))

    df34.show()


    println("Q35 Display employee salary and as per highest  salary provide Grade to employee ")

    val window_fun=Window.orderBy(col("E_Sal").desc)

   val df35=df_emp.withColumn("grade",dense_rank().over(window_fun))

    df35.select("EName","E_Sal","grade").show()


    println("Q36  display the 4th record of emp table without using group by and rowid")
   val windoe_f=Window.orderBy(col("EID").asc)

    val df36=df_emp.withColumn("rank",row_number().over(windoe_f)).filter(col("rank")===4).select("*")

      df36.show()


    println("Q37 Provide a commission 100 to employees who are not earning any commission.")
    val df37 = df_emp.join(df_ser, Seq("EID"), "inner")
      .withColumn("Comm_new", when(col("Comm").isNull || col("Comm") === 0, lit(100)).otherwise(col("Comm")))
      .select("EName", "Comm_new")

    df37.show()


    println("Q38 write a query that totals no. of services  for each day and place the results\nin descending order")

    val df38=df_emp.join(df_ser,Seq("EID"))
      .groupBy("Ser_Date")
      .agg(count(col("*")).alias("ser_count"))
      .orderBy(desc("ser_count"))

    df38.show()


    println("Q39 Display the service details of those" +
      " customer who belong from same city ")
    val duplicate_cities=df_cust
      .groupBy("CAdd")
      .agg(count(col("*")).alias("city_count"))
      .filter(col("city_count")>1).select("CAdd")

   val df39=df_cust
     .join(df_ser,Seq("CID"),"inner")
     .join(duplicate_cities,Seq("CAdd"),"inner")
     .select(df_cust("CName"),df_cust("CAdd"),df_ser("*"))

    df39.show()



    println("Q40 write a query join customers table to itself to find all pairs of\ncustomers service by a single employee")

    val cust_ser=df_cust.join(df_ser,Seq("CID"),"inner")

  val df40=cust_ser.as("c1")
    .join(cust_ser.as("c2"),
    (col("c1.EID")===col("c2.EID"))
    && (col("c1.CID")<col("c2.CID")))
    .select(
      col ("c1.CID").alias("cust_ID"),
      col("c1.CName").alias("cust_1"),
      col("c2.Cname").alias("cust_2")
    )
    .distinct()

    df40.show()



    println("Q41 List each service number follow by name of the customer who\nmade  that service")
    val df41=df_ser.join(df_cust,Seq("CID"),"left").join(df_emp,Seq("EID"),"inner")select("SID","CName","EName")

    df41.show()



    println("Q42 Write a query to get details of employee and provide rating on basis of  maximum services provide by employee  ." +
      "Note (rating should be like A,B,C,D)")
//    val window_rating=Window.partitionBy("EID")
//    val df_rank=df_emp.join(df_ser,Seq("EID"),"inner").withColumn("rank",row_number().over(window_rating))
//   val resulr=df_rank.groupBy("Typ_Ser").


    println("Q43 Write a query to get maximum service amount of each customer with their customer details ")
    val max_ser=df_ser.join(df_cust,Seq("CID"),"inner").groupBy("CName","CID")
      .agg(max(col("Total")).alias("max_amt")).first().getAs[Double]("Max_amt")

    val df43=df_ser.join(df_cust,Seq("CID"),"inner").filter(col("Total")===max_ser).select(df_cust("*"))
   df43.show()

    println("Q44 Get the details of customers with his total no of services ?")
    val count_ser=df_ser.join(df_cust,Seq("CID"),"inner").groupBy("CName","CID").
      agg(count(col("*")).alias("total_ser")).first().getAs[Double]("total_ser")

    val df44=df_ser.join(df_cust,Seq("CID"),"inner").filter(col("SID")===count_ser).select(df_cust("*"))

   df44.show()

    println("Q45 From which location sparpart purchased  with highest cost ?")
    val high_cost=df_pur.join(df_vendor,Seq("VID"),"inner")
      .agg(max("Total")).alias("high_cost").first().getAs[Double]("high_cost")

    val df45=df_pur.join(df_vendor,Seq("VID"),"inner").filter(col("Total")===high_cost).select("VAdd")

    df45.show()

    println("Q46 Get the details of employee with their service details who has salary is null")
  val df46=df_emp.join(df_ser,Seq("EID"),"inner").filter(col("E_Sal").isNull).select(df_emp("*"),df_ser("*"))

  df46.show()


    println("Q47 find the sum of purchase location wise ")
    val sum_amt=df_pur.join(df_vendor,Seq("VID"),"inner").groupBy("VAdd")
      .agg(sum("Total")).alias("total_cost")

   sum_amt.show()

    println("Q48 write a query sum of purchase amount in word location wise ?")




//    println("Q49 Has the customer who has spent the largest amount money has\nbeen give highest rating")
//   val high_amt=df_cust.join(df_ser,Seq("CID"),"inner")
//     .agg(max(col("Total")).alias("high_pay").desc)
//
//    val win_f=Window.orderBy(col("high_pay").desc)
//
//    val df49=high_amt.withColumn("rating")

    println("Q50 select the total amount in service for each customer for which\nthe total is greater than the amount of the largest service amount in the table")
    val count_cust= df_ser.join(df_cust,Seq("CID"),"inner").agg(max(col("Total")).alias("total_amt")).first().getAs[Double]("total_amt")

    val df50=df_ser.join(df_cust,Seq("CID"),"inner").filter(col("Total")>count_cust).select("CName","total_amt")



      println("Q51  List the customer name and sparepart name used for their vehicle and  vehicle type")
val df51=df_cust.join(df_ser,Seq("CID"),"inner").join(df_spare,Seq("SPID"),"inner").select("CName","SPName")
    df51.show()


    println("Q52 Write a query to get spname ,ename,cname quantity ,rate ,service amount for record exist in service table")
    val df52=df_cust.join(df_ser,Seq("CID"),"inner")
      .join(df_spare,Seq("SPID"),"inner")
      .join(df_emp,Seq("EID"),"inner")
      .join(df_spare,Seq("SPID"),"inner").select("CName","SPName","EName","Ser_Amt")

    df52.show()



    println("Q53 specify the vehicles owners who’s tube damaged.")
    val df53=df_cust.join(df_ser,Seq("CID"),"inner")
      .filter(col("Typ_Ser")===("Tube Damaged")).select(df_cust("*"))

    df53.show()



    println("Q.54 Specify the details who have taken full service.")

    val df54=df_cust.join(df_ser,Seq("CID"),"inner")
      .filter(col("Typ_Ser")===("Full Servicing")).select(df_cust("*"))

    df54.show()


    println("Q.55 Select the employees who have not worked yet and left the job.")
val df55=df_emp.join(df_ser,Seq("EID"),"left")
  .filter(col("df_ser").isNull && col("E_Dol").isNotNull)
  .select("EName")


    println("Q.56  Select employee who have worked first ever.")
    val df_count_e=df_emp.join(df_ser,Seq("EID"),"left").groupBy("EID").agg(count(col("*")).alias("emp_count")).filter(col("emp_count")===1)

  val df56=df_emp.join(df_ser,Seq("EID"),"left").filter(col("EID")===df_count_e).select("EName")


    println("Q.57 Display all records falling in odd date")
df_cust.filter(col("CJ_Date")%2===1).show()
    df_pur.filter(col("PDate")%2===1).show()
    df_ser.filter(col("Ser_Date")%2===1).show()
    df_vendor.filter(col("VJ_Date")%2===1).show()


    println("Q.58 Display all records falling in even date")

    df_cust.filter(col("CJ_Date")%2===0).show()
    df_pur.filter(col("PDate")%2===0).show()
    df_ser.filter(col("Ser_Date")%2===0).show()
    df_vendor.filter(col("VJ_Date")%2===0).show()


    println("Q.59 Display the vendors whose material is not yet used.")
val df59=df_vendor.join(df_pur,Seq("VID"),"inner").
  join(df_ser,Seq("SPID"),"left").filter(col("df_ser").isNull)
    df59.show()


    println("Q.60 Difference between purchase date and used date of spare part.")
    val df60=df_pur.join(df_spare,Seq("SPID"),"inner")
      .withColumn("days_diff",datediff(col("Ser_date"),col("PDate")))
      df60.select("SPID","PDate","Ser_Date","days_diff").show()





  }
}



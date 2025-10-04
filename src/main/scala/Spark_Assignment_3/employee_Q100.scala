package Spark_Assignment_3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.ScalaReflection.universe.show
import org.apache.spark.sql.functions.{avg, col, count, length, max, min, sum, when}
import org.apache.spark.sql.types.{DecimalType, IntegerType, StringType, StructField, StructType}

import scala.Console.println

object employee_Q100 {
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
    df1.printSchema()

  println("1. Select all columns from the `employees` table.")
    df.show()

    println("2. Select the first name and last name of all employees.")
    df.select("first_name", "last_name").show()


    println("3. Select the employees who have a salary greater than 70000.")
    df.filter(col("salary")>70000).show()

    println("4. Select the employees who work in the 'IT' department.")
    df.filter(col("dept")==="IT").show()

    println("5. Select the employees who are older than 30.")
    df.filter(col("age")>30).show()


    println("6. Select the employees who are male and work in 'HR'.")
    df.filter(col("gender")==="Male" && col("dept")==="HR").show()

    println("7. Select the employees who live in 'Mumbai'.")
    df.filter(col("city")>"Mumbai").show()

    println("8. Select the employees who have a salary between 60000 and 70000.")
    df.filter(col("salary").between(60000,70000)).show()

    println("9. Select the employees whose first name starts with 'S'")
    df.filter(col("first_name").like("S%")).show()

    println("10. Select the employees whose age is less than 28.")
    df.filter(col("age")>30).select("first_name","last_name").show()

    println("11. Select the employees whose last name contains 'Pat'.")
    df.filter(col("last_name").like("%Pat")).select("first_name","last_name").show()


    println("12. Select the employees whose first name ends with 'a'.")
    df.filter(col("last_name").like("%a")).select("first_name","last_name").show()


    println("13. Select the employees whose department starts with 'M'.")
    df.filter(col("dept").like("M%")).select("first_name","last_name").show()


    println("14. Select the employees whose city contains 'na'.")
    df.filter(col("city").like("%na%")).select("first_name","last_name").show()



    println("15. Select the employees whose first name is four characters long.")
    df.filter(col("first_name").like("____")).select("first_name","last_name").show()


    println("16. Select the employees whose last name starts with 'K' and ends with 'e'.")
    df.filter(col("last_name").like("K%e")).select("first_name","last_name").show()



    println("17. Select the employees whose first name contains 'a' and is more than 5 characters long.")
    df.filter(col("first_name").contains("%a%") && length(col("first_name"))>5).select("first_name","last_name").show()


    println("18. Select the employees whose last name starts with 'S' and contains 'h'.")
    df.filter(col("last_name").like("S%h")).select("first_name","last_name").show()


    println("19. Select the employees whose city ends with 'ur'.")
    df.filter(col("city").like("%ur")).select("first_name","last_name").show()



    println("20. Select the employees whose department contains 'i' but does not start with 'F'.")
    df.filter(col("dept").like("%i%") && !col("dept").like("F%")).select("first_name","last_name").show()



    println("21. Select all employees and display their salary with an additional 10% if they are in the 'Marketing' department.")
    df.withColumn("updated_salary",when(col("dept")==="Marketing",col("Salary")*1.1.toInt).otherwise(col("salary"))).show()


    println("22. Select all employees and display 'Senior' if their age is greater than 30, otherwise 'Junior'.")
    df.withColumn("level",when(col("age")>30,"senior").otherwise("Junior")).show()


    println("23. Select all employees and display 'High' if their salary is greater than 70000, 'Medium' if it is between 60000 and 70000, and 'Low' otherwise.")
    df.withColumn("salary_level",when(col("salary")>70000,"High").when(col("salary").between(60000,70000),"Low").otherwise("Low")).show()

    println("24. Select all employees and display 'A' if their first name starts with 'A', 'B' if it starts with 'B', and 'Other' otherwise.")
    df.withColumn("prefix",when(col("first_name").like("A%"),"A").when(col("first_name").like("B%"),"B").otherwise("Other")).show()


    println("25. Select all employees and display 'Yes' if they are male, otherwise 'No'.")
    df.withColumn("Y/N",when(col("gender")==="Male","Yes").otherwise("No")).show()


    println("26. Find the total salary for each department.")
    df.groupBy("dept").agg(sum("salary").alias("total_salary")).show()

    println("27. Find the average salary for each city.")
    df.groupBy("city").agg(avg("salary").alias("avg_salary")).show()


    println("28. Count the number of employees in each department.")
    df.groupBy("dept").agg(count("salary").alias("no_of_employees")).show()

    println("29. Find the maximum salary in each department.")
    df.groupBy("dept").agg(max("salary").alias("max_salary")).show()


    println("30. Find the minimum salary in each city.")
    df.groupBy("city").agg(min("salary").alias("min_salary")).show()


    println("31. Find the average age of employees in each department.")
    df.groupBy("dept").agg(avg("age").alias("total_salary")).show()


    println("32. Count the number of male and female employees in each city.")
    df.groupBy("city","Gender").agg(count("*").alias("gender_count")).show()


   println("33. Find the total salary for each city where the total salary is greater than 200000.")
    df.groupBy("city").agg(sum("salary").alias("total_salary")).filter(col("total_salary")>20000).show()


    println("34. Find the average salary in each department where the department has more than 5 employees.")
    df.groupBy("dept").agg(avg("salary")alias("avg_salary"),count("*").alias("no_of_employees")).filter(col("no_of_employees")>5).show()


    println("35. Count the number of employees in each department who are older than 30.")
    df.filter(col("age")>30).groupBy(col("dept")).agg(count("*").alias("no_of_employees")).show()

    println("36. Find the total number of employees in each city.")
    df.groupBy("city").agg(count("*").alias("emp_count")).show()


    println("37. Find the average salary for each department where the average salary is greater than 65000.")
    df.groupBy("dept").agg(avg("salary").alias("avg_salary")).filter(col("avg_salary")>65000).show()


    println("38. Count the number of employees in each city who have a salary greater than 60000.")
    df.filter(col("salary")>60000).groupBy("city").agg(count("*").alias("emp_count")).show()


    println("39. Find the total salary for each department where the total salary is less than 300000.")
    df.groupBy("dept").agg(sum("salary").alias("total_salary")).filter(col("total_salary")<30000).show()



    println("40. Find the maximum age in each department.")
    df.groupBy(col("dept")).agg(max(col("age")).alias("max_age")).show()


    println("41. Count the number of employees in each city who are younger than 30.")
    df.filter(col("age")<30).groupBy(col("city")).agg(count("*").alias("no_of_employees")).show()


   println("42. Find the total salary for each city where the total salary is greater than 250000.")
    df.groupBy(col("city")).agg(sum(col("salary")).alias("total_salary")).filter(col("total_salary")>250000).show()


    println("43. Find the average age in each department where the average age is greater than 30.")
    df.groupBy(col("dept")).agg(avg("age").alias("avg_age")).filter(col("avg_age")>30).show()


    println("44. Count the number of employees in each department who have a salary less than 65000.")
    df.filter(col("salary")<65000).groupBy("dept").agg(count("*").alias("emp_count")).show()

    println("45. Find the total salary for each city where the total salary is less than 300000.")
    df.groupBy(col("city")).agg(sum("salary").alias("total_salary")).filter(col("total_salary")<30000).show()


    println("46. Find the average age in each city where the average age is greater than 28.")
    df.groupBy("city").agg(avg("age").alias("avg_age")).filter(col("avg_age")>28).show()


    println("47. Count the number of employees in each department who have a salary between 60000 and 70000.")
    df.filter(col("salary").between(60000,70000)).groupBy("dept").agg(count("*").alias("emp_count")).show()


    println("48. Find the total salary for each department where the total salary is greater than 250000.")
    df.groupBy(col("dept")).agg(sum(col("salary")).alias("total_salary")).filter(col("total_salary")>250000).show()


    println("49. Find the average salary in each department where the average salary is greater than 68000.")
    df.groupBy(col("dept")).agg(avg(col("salary")).alias("avg_salary")).filter(col("avg_salary")>68000).show()


    println("50. Count the number of employees in each city who have a salary less than 60000.")
    df.filter(col("salary")<60000).groupBy("city").agg(count("*").alias("emp_count")).show()


    println("51. Select all employees and order them by salary in ascending order.")
    df.orderBy(col("salary").asc).show()


    println("52. Select all employees and order them by age in descending order.")
    df.orderBy(col("age").desc).show()


    println("53. Select all employees and order them by department and then by salary in descending order.")
    df.orderBy((col("dept").desc),col("salary").desc).show()


    println("54. Select all employees and order them by city and then by first name in ascending order.")
    df.orderBy((col("city").desc),col("first_name").asc).show()


    println("55. Select all employees and order them by gender and then by last name in descending order.")
    df.orderBy((col("gender").desc),col("last_name").desc).show()


    println("56. Select all employees and order them by salary in descending order and age in ascending order.")
    df.orderBy((col("salary").asc),col("age").asc).show()


    println("57. Select all employees and order them by first name and then by department in ascending order.")
    df.orderBy((col("first_name").asc),col("dept").asc).show()


    println("58. Select all employees and order them by last name and then by city in descending order.")
    df.orderBy((col("last_name").desc),col("city").desc).show()


    println("59. Select all employees and order them by age and then by salary in ascending order.")
    df.orderBy((col("age").asc),col("salary").asc).show()


    println("60. Select all employees and order them by department and then by first name in descending order.")
    df.orderBy((col("dept").desc),col("first_name").desc).show()


    println("61. Select all employees who have a salary greater than the average salary of their department.")


    println("62. Select all employees who are older than the average age of their department.")




    println("63. Select all employees whose salary is greater than the average salary of their city.")




    println("64. Select all employees whose age is less than the average age of their city.")



    println("65. Select all employees who have a salary greater than the average salary of the entire company.")



    println("66. Select all employees who are older than the average age of the entire company.")




    println("67. Find the total salary of all employees and display it as 'Total Salary'.")



    println("68. Find the average age of all employees and display it as 'Average Age'.")



    println("69. Count the total number of employees and display it as 'Total Employees'.")



    println("70. Find the maximum salary of all employees and display it as 'Max Salary'.")



    println("71. Find the minimum age of all employees and display it as 'Min Age'.")



    println("72. Select the first name and last name of the employee with the highest salary.")



    println("73. Select the first name and last name of the oldest employee.")



    println("74. Select the first name and last name of the youngest employee.")



    println("75. Select the first name and last name of the employee with the lowest salary.")



    println("76. Find the total salary of all employees who are older than 30 and display it as 'Total Salary'.")



    println("77. Find the average age of all employees who have a salary greater than 60000 and display it as 'Average Age'.")



    println("78. Count the total number of employees who work in 'IT' and display it as 'Total IT Employees'.")



    println("79. Find the maximum salary of all employees who work in 'HR' and display it as 'Max HR Salary'.")



    println("80. Find the minimum age of all employees who work in 'Finance' and display it as 'Min Finance Age'.")



    println("81. Select the first name and last name of the employee with the highest salary in 'Marketing'.")



    println("82. Select the first name and last name of the oldest employee in 'IT'.")



    println("83. Select the first name and last name of the youngest employee in 'HR'.")



    println("84. Select the first name and last name of the employee with the lowest salary in 'Finance'.")



    println("85. Find the total salary of all employees who work in 'Mumbai' and display it as 'Total Mumbai Salary'.")



    println("86. Find the average age of all employees who work in 'Pune' and display it as 'Average Pune Age'.")



    println("87. Count the total number of employees who work in 'Nagpur' and display it as 'Total Nagpur Employees'.")



    println("88. Find the maximum salary of all employees who work in 'Nashik' and display it as 'Max Nashik Salary'.")



    println("89. Find the minimum age of all employees who work in 'Mumbai' and display it as 'Min Mumbai Age'.")



    println("90. Select the first name and last name of the employee with the highest salary in 'Pune'.")


    println("91. Select the first name and last name of the oldest employee in 'Nagpur'.")



    println("92. Select the first name and last name of the youngest employee in 'Nashik'.")



    println("93. Select the first name and last name of the employee with the lowest salary in 'Mumbai'.")



    println("94. Find the total salary of all male employees and display it as 'Total Male Salary'.")



    println("95. Find the average age of all female employees and display it as 'Average Female Age'.")



    println("96. Count the total number of male employees and display it as 'Total Male Employees'.")



    println("97. Find the maximum salary of all female employees and display it as 'Max Female Salary'.")



    println("98. Find the minimum age of all male employees and display it as 'Min Male Age'.")



    println("99. Select the first name and last name of the male employee with the highest salary.")



    println("100. Select the first name and last name of the female employee with the lowest salary.")



    println("")



    println("")


    println("")






































  }


}

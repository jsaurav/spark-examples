// Source code for Chapter 6

// Dataframe is just a Dataset of type row.
//val DataFrame = Dataset<Row>

// Dataset submodules.
// DataFrameStatFunctions
//
 DataFrameNaFunctions

// org.apache.spark.sql.functions.* - contains functions to deal with different data types.

// Transform rows of data in one format or structure to another.

// lit function - this function converts a type in language to its equivalent spark representation.

// Let's create a dataframe.
val df = spark.read.format("csv")
.option("inferSchema","true")
.option("header","true")
.load("/Users/saurabhjain/soft/spark-2.3.2-bin-hadoop2.7/customdata/Spark-The-Definitive-Guide/data/retail-data/by-day/2010-12-01.csv")

// Create a table
df.createOrReplaceTempView("dfTable")

spark.sql(""" select * from dfTable where InvoiceNo=536365 """)

df.where(col("InvoiceNo") > 1000).show(5)

// ANother way or most cleanest way
df.where("InvoiceNo < 10000").show(5)

// In spark you should always chain together "and" filter as sequential filter.
// or statements need to be specified in the same statement.

// filter and where can be used interchangeably

// Let's create filters.
val priceFilter = col("UnitPrice") > 600
val descFilter = col("Description").contains("WHITE")

df.where(col("StockCode").isin("DOT")).where(priceFilter.or(descFilter)).show(5)

// doing an and between priceFilter and descFilter

df.where(col("StockCode").isin("DOT")).where(priceFilter).where(descFilter)

// and filters as sequential filter.
// or statements need to be specified in the same statement.

// To filter a data frame a boolean column can also be used.

val dotCodeFilter = col("StockCode") === "DOT"
val ffV1 = df.withColumn("is_expensive", dotCodeFilter.and(priceFilter.or(descFilter)))
.filter(expr("is_expensive"))
.select("*")


// Number Data Types
import org.apache.spark.sql.functions.POWER

val ffC1 = df.selectExpr("InvoiceNo","StockCode", "(pow((Quantity * UnitPrice),2) + 5) as RealQuantity", "Quantity")
ffC1.show(5)

// Perason Correlation Coefficient can also be checked.

// What is approx quantile ?

// To add a unique id to each row , we can use the function montonoically_increasing_ids

// Play with Strings

import org.apache.spark.sql.functions.regexp_replace
val simpleColors = Seq("black","white","red","blue","green")
val regex = simpleColors.map(_.toUpperCase).mkString("|")
df.select(regexp_replace(col("Description"), regex, "SAURABH").alias("color_clean"), col("Description")).show()


// Play with date and timestamps

import org.apache.spark.sql.functions.{current_date,current_timestamp}

val dateDF = spark.range(10).withColumn("today", current_date).withColumn("now",current_timestamp)
dateDF.printSchema()

// Add a date or subtract a date.

import org.apache.spark.sql.functions.{date_add,date_sub}
val dateDFM1 = dateDF.select(date_add(col("today"), 5) as "Add Ex", date_sub(col("today"), 10) as "Sub Ex")
dateDFM1.show()

// Find days between 2 dates , find months between 2 days,
import org.apache.spark.sql.functions.{datediff, months_between, to_date}
val dateDFM2 = dateDF.withColumn("7 days later", date_add(col("today"), 7))
.select(col("7 days later"), datediff(col("today"), col("7 days later")) as "Difference").show()


// Working with nulls in the data.
// There are 2 things that you can do with the null data , either you cna replace it with a good value globally or
// per column basis or you can drop the nulls.

// coalesce
import org.apache.spark.sql.functions.coalesce
df.select(coalesce(col("Description"), col("CustomerId"))).show()

//drop
df.na.drop("any") // if any column has null value
df.na.drop("all") // if all the columns have null value
df.na.drop("all" , Seq(col("Description"), col("CustomerId")))

//fill
//Using fill we can fill column with any value in case it is null.
df.na.fill("The value in the column was null.")

//replace
//ordering
// -- asc_nulls_first , desc_nulls_first , asc_nulls_last , desc_nulls_last

// Complex Types
// Struct , Arrays and Maps
// Structs - Dataframes within dataframes.

// Creating data frame again
val df = spark.read.format("csv")
.option("inferSchema","true")
.option("header","true")
.load("/Users/saurabhjain/soft/spark-2.3.2-bin-hadoop2.7/customdata/Spark-The-Definitive-Guide/data/retail-data/by-day/2010-12-01.csv")

// inline way
df.selectExpr("struct(Description, InvoiceNo)", "CustomerId").show(5)

import org.apache.spark.sql.functions.struct


val dfComplex = df.select(struct("Description", "InvoiceNo").alias("Complex"))
dfComplex.show(1)

dfComplex.select("Complex.InvoiceNo").show(2)
dfComplex.select(col("Complex").getField("Description")).show(2)


// Arrays

//explode
// exploed function takes a columns which contains an array of valued created by using split and then
// create a new row for each of the value in the array while duplicating the value of other
// columns.
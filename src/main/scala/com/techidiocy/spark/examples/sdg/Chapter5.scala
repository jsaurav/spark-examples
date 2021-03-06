// Basic Structured Operations

val df = spark.read.format("json").load("C:\\saurav\\git-repo\\Spark-The-Definitive-Guide\\data\\flight-data\\json\\2015-summary.json")

val mdf = spark.read.format("json").load("/Users/saurabhjain/soft/spark-2.3.2-bin-hadoop2.7/customdata/Spark-The-Definitive-Guide/data/flight-data/json/2015-summary.json")

mdf.show()

val df2 = spark.read.json("C:\\saurav\\git-repo\\Spark-The-Definitive-Guide\\data\\flight-data\\json\\2015-summary.json")

// Schema defines the column names and types of a data frame.

// get the schema from the data frame.

spark.read.json("C:\\saurav\\git-repo\\Spark-The-Definitive-Guide\\data\\flight-data\\json\\2015-summary.json").schema


// org.apache.spark.sql.types.StructType = ...
// StructType(
//				StructField(DEST_COUNTRY_NAME,StringType,true),
// 			    StructField(ORIGIN_COUNTRY_NAME,StringType,true),
// 			    StructField(count,LongType,true)
//			)

// You can select , manipulate , remove columns from dataframes and these operations are knows as Expressions.

// Get the first record out of the data frame.
df.first

// Create a row
import org.apache.spark.sql.Row
val myRow = Row("Saurabh","Jain",32)

// DataFrame Operations
//1. Add rows or columns to a DataFrame.
//2. Remove rows or columns from a DataFrame.
//3. Transform a row into a column or vice-versa.
//4. Sort the data by values in rows.

// Create a DataFrame Manually.
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}

// Create schema
val mySchema = new StructType(Array(new StructField("firstName",StringType, true),
									new StructField("lastName", StringType, true),
									new StructField("age", IntegerType, true)))

// Create data
val rows = Seq(Row("Saurabh", "Jain", 32), Row("Adam", "G", 45))

// Create RDD
val personRDD = spark.sparkContext.parallelize(rows)

// Create DataFrame from RDD
val personDF = spark.createDataFrame(personRDD, mySchema)
personDF.show()

//Create DF from the sequence.
val seqs = Seq(("Saurabh", "Jain", 32), ("Adam", "G", 45))
val personDF2 = seqs.toDF()
personDF2.show()

// On Mac
val mdf = spark.read.format("json").load("/Users/saurabhjain/soft/spark-2.3.2-bin-hadoop2.7/customdata/Spark-The-Definitive-Guide/data/flight-data/json/2015-summary.json")
mdf.show()

mdf.select("DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME").show(15)

mdf.selectExpr("DEST_COUNTRY_NAME as destination", "DEST_COUNTRY_NAME").show(10)

val mdfNewCol = mdf.selectExpr("*", "(DEST_COUNTRY_NAME=ORIGIN_COUNTRY_NAME) as within_same_country")
mdfNewCol.show(10)

mdfNewCol.createOrReplaceTempView("mdftable")

import org.apche.spark.sql.functions.{expr,col,column}

// Add column
import org.apache.spark.sql.functions.lit
mdf.withColumn("NumberOne", lit("1"))

// Column Renaming
mdf.withColumn("DEST_COUNTRY_NAMEXXXXX", expr("DEST_COUNTRY_NAME")).show(5)
val mdfm = mdf.withColumnRenamed("DEST_COUNTRY_NAME", "HELLO  WORLD").show(5)

// Drop a column
mdfm.drop("HELLO  WORLD")

mdf.selectExpr("DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME", "count").filter(col("count") > 10).show(5)

// Get unique or distinct values

mdf.select("DEST_COUNTRY_NAME","ORIGIN_COUNTRY_NAME").distinct().show(100)

// Sampling of actual data.
mdf.count()
mdf.sapmple(false, .5, .5).count()


// Partitioning
val dfs = mdf.randomSplit(Array(0.3,0.7), 5)
dfs(0).count()








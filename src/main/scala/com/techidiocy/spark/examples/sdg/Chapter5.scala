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
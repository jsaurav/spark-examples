# Chapter 3 - Structured Streaming

# Create a static data frame
val staticDataFrame = spark.read.format("csv").option("header","true").option("inferSchema", "true").load("c:\\saurav\\git-repo\\Spark-The-Definitive-Guide\\data\\retail-data\\by-day\\*.csv")

# Create a temp view out of the data frame.
staticDataFrame.createOrReplaceTempView("static_data")

# Get the schema out of the dataframe.
val staticSchema = staticDataFrame.schema

import org.apache.spark.sql.functions.{window, column, desc, col}

staticDataFrame.selectExpr("CustomerId","(UnitPrice * Quantity) as total_cost", "InvoiceDate")
.groupBy( col("CustomerId"), window(col("InvoiceDate"), "1 day"))
.sum("total_cost").show(5)

// spark.sql(""" select CustomerId, (UnitPrice * Quantity) as total_cost, InvoiceDate from static_data group by CustomerId """)

spark.conf.set("spark.sql.shuffle.partitions", "5")

val streamingDataFrame = spark.readStream.format("csv")
.schema(staticSchema)
.option("header","true")
.option("maxFilesPerTrigger",1)
.load("c:\\saurav\\git-repo\\Spark-The-Definitive-Guide\\data\\retail-data\\by-day\\*.csv")

// Check if data frame is streaming or not
streamingDataFrame.isStreaming

// Create a new data frame that will have our actial business logic.
val purchaseByCustomerPerHour = streamingDataFrame.selectExpr("CustomerId","(UnitPrice * Quantity) as total_cost", "InvoiceDate")
.groupBy( col("CustomerId"), window(col("InvoiceDate"), "1 day"))
.sum("total_cost")

// Start the stream.

purchaseByCustomerPerHour.writeStream.format("console").queryName("cpq1").outputMode("append").start()
purchaseByCustomerPerHour.writeStream.format("memory").queryName("cpq2").outputMode("append").start()

spark.sql(""" select * from cpq2 order by `sum(total_cost)` DESC """).show(10)
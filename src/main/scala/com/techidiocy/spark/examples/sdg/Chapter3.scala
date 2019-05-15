# Chapter 3 - Structured Streaming

# Create a static data frame from the retail data
val staticDataFrame = spark.read.format("csv").option("inferSchema","true").option("header","true").load("../customdata/Spark-The-Definitive-Guide/data/retail-data/by-day/*.csv")

# Create a temp view or table.
staticDataFrame.createOrReplaceTempView("retail_data")

# Get the schema out of the table or view.
val staticSchema = staticDataFrame.schema
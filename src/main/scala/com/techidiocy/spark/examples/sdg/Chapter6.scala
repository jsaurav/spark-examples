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

df.where(col("InvoiceNo") > 1000).show(5)

// ANother way or most cleanest way
df.where("InvoiceNo < 10000").show(5)

// In spark you should always chain together "and" filter as sequential filter.
// or statements need to be specified in the same statement.

// Let's create filters.
val priceFilter = col("UnitPrice") > 10
val descFilter = col("Description").contains("WHITE")

df.where(col("StockCode").isin("DOT")).where(priceFilter.or(descFilter)).show(5)

// doing an and between priceFilter and descFilter

df.where(col("StockCode").isin("DOT")).where(priceFilter).where(descFilter)

// and filters as sequential filter.
// or statements need to be specified in the same statement.

// To filter a data frame a boolean column can also be used.
// Basic Structured Operations

val df = spark.read.format("json").load("C:\\saurav\\git-repo\\Spark-The-Definitive-Guide\\data\\flight-data\\json\\2015-summary.json")

val df2 = spark.read.json("C:\\saurav\\git-repo\\Spark-The-Definitive-Guide\\data\\flight-data\\json\\2015-summary.json")

// Schema defines the column names and types of a data frame.

// get the schema from the data frame.

spark.read.json("C:\\saurav\\git-repo\\Spark-The-Definitive-Guide\\data\\flight-data\\json\\2015-summary.json").schema


// org.apache.spark.sql.types.StructType = ...
// StructType(StructField(DEST_COUNTRY_NAME,StringType,true),
// StructField(ORIGIN_COUNTRY_NAME,StringType,true),
// StructField(count,LongType,true))
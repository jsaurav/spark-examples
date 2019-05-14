// Create a DataFrame from the flight data
val flightData2015 = spark.read.option("inferSchema", "true").option("header","true").csv("/Users/saurabhjain/soft/spark-2.3.2-bin-hadoop2.7/customdata/Spark-The-Definitive-Guide/data/flight-data/csv/2015-summary.csv")

// View the data.
flightData2015.take(10)

// Sort the data.
val flightData2015Sorted = flightData2015.sort("count")	

// Create a temp view or table

flightData2015.createOrReplaceTempView("flight_data_2015")

// Getting number of flights grouped by dest_country_name in the sql way
val sqlWay = spark.sql(""" select dest_country_name , count(1) from flight_data_2015 group by dest_country_name """)

// Trigger the action
sqlWay.collect()

// Getting number of flights grouped by dest_country_name in the data frame way
val dfWay = flightData2015.groupBy("dest_country_name").count()

// Trigger the action
dfWay.collect()


// Take the row with max number of flights
val maxSqlWay = spark.sql(""" select max(count) from flight_data_2015  """).take(1)
maxSqlWay.collect()


flightData2015.select(max("count")).take(1)

// Get the top 10 countries which has the max flight in sqlWay

val sqlTop10 = spark.sql(""" select dest_country_name, sum(count) as total from flight_data_2015 group by dest_country_name order by  sum(count) desc """)
// Trigger an action
sqlTop10.show()

// Get the top 10 countries which has the max flight in dfWay

val dfTop10 = flightData2015.groupBy("dest_country_name").sum("count").withColumnRenamed("sum(count)", "total").sort(desc("total")).limit(10)
dfTop10.show()


package com.techidiocy.spark.examples

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object WordCount extends App {
  
  //Creates the config object , with appname as "WordCount" and cluster type as "local" 
  val conf = new SparkConf().setAppName("WordCount").setMaster("local")
  
  //Creates the spark context object
  val sc = new SparkContext(conf)
  
  //Creates the inputLinesRDD by reading a text file.
  val inputLinesRDD =  sc.textFile("c:/saurav/docs/scratch_pad")
  
  //Using the flat map , splitted all the words using space tokenizer
  val wordsRDD = inputLinesRDD.flatMap(line => line.split(" "))
  
  //Using map transformation creates a new key/value pair rdd and 
  //initialized counter for all the words as 1.
  val mappedWords = wordsRDD.map(word => (word,1))
  
  //Finally used the reduceByKey transformation to do the summation
  val result = mappedWords.reduceByKey{ case(x,y) => x + y }
  
  //Action which caused the whole DAG to trigger.
  result.foreach(println)
}
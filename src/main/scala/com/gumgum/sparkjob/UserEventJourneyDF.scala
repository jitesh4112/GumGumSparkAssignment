package com.gumgum.sparkjob

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object UserEventJourneyDF {

	def main(args: Array[String]) {

		// Set the log level to only print errors
		Logger.getLogger("org").setLevel(Level.ERROR)

		var inputFilePath = ""
		var outputFilePath = ""
		
		if(args.length == 2){
			inputFilePath = args(0).toString()
			outputFilePath = args(1).toString()
		}else{
		  println("Please give input File Path as 1st argument... and putput File Path as 2nd argument")
		  return;
		}
		
		val spark = SparkSession
		.builder
		.appName("User Journey DF")
		.master("local[*]")
		.getOrCreate()
		
		//Loading the data from file and filtering if visitorId is absent
		import spark.implicits._
		val df = spark.read.json(inputFilePath).filter(_.getAs("visitorId") != null).filter(_.getAs("pageUrl") != null).filter(_.getAs("timestamp") != null)

		println(df.count()+" event lines have visitor Ids.....")
		println(df.select("visitorId").distinct().count() + " unique visitors.....")
		//println(df.schema)

		//transforming the DataFrame with adding newpageUrl for user event
		val newDF = df.transform(addnextpageUrl())
		
		println(newDF.count()+" lines are processed for output.....")
		//saving User Journey in File as JSON String
		newDF.repartition(1).toJSON.rdd.saveAsTextFile(outputFilePath)

		//newDF.toJSON.take(100).foreach(println)
		/*newDF.foreach{r => 
		  println(r)
		}*/
		println(s"OutPut is Stored in "+outputFilePath)
		spark.stop()
	}
	
	def addnextpageUrl()(df: DataFrame): DataFrame = {
    //creating the window for grouping visitorId and type with the orderBy time
		val window = Window.orderBy("timestamp").partitionBy("visitorId")

		//creating lead to add new colunm of nextPageUrl with value of next user's activity
		val leadCol = lead(col("pageUrl"), 1,"null").over(window)

		//adding new column in DF and creating new DF with seq select query
		val newDF = df.withColumn("nextPageUrl", leadCol).select("id","timestamp","type","visitorId","pageUrl","nextPageUrl")
		
		newDF
  }
}

package com.gumgum.sparkjob

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.Window

object UserEventJourneyDS {

  final case class AdEvent(id: String, timestamp: String, `type`: String, visitorId: String, pageUrl: String)
  
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
		.appName("User Journey DS")
		.master("local[*]")
		.getOrCreate()
		
		//Loading the data from file and filtering if visitorId is absent
		import spark.implicits._
		val ds = spark.read.json(inputFilePath).as[AdEvent]
		.filter($"visitorId" =!=("") ||  $"pageUrl" =!= "" || $"timestamp" =!= "")

		println(ds.count())
		//println(df.schema)

		//creating the window for grouping visitorId and type with the orderBy time
		val window = Window.orderBy("timestamp").partitionBy("visitorId")

		//creating lead to add new colunm of nextPageUrl with value of next user's activity
		val leadCol = lead(col("pageUrl"), 1,"null").over(window)

		//adding new column in DF and creating new DF with seq select query
		val newDS = ds.withColumn("nextPageUrl", leadCol).select("id","timestamp","type","visitorId","pageUrl","nextPageUrl")

		println(newDS.count())
		//saving User Journey in File as JSON String
		//newDS.toJSON.rdd.saveAsTextFile(outputFilePath)

		//newDS.toJSON.take(100).foreach(println)
		/*newDS.foreach{r => 
		  println(r)
		}*/
		println(s"OutPut is Stored in "+outputFilePath)
		spark.stop()
	}
}
package com.gumgum.sparkjob

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.rdd.RDD
import scala.collection.immutable.ListMap

object UserEventJourneyRDD {
  
  final case class AdEvent(id: String, timestamp: String, `type`: String, visitorId: String, pageUrl: String, nextPageUrl: String)

  //adding new record of user's next page URL
	def nextPageUrl(value : Map[Long,String]) : List[AdEvent] = {

			var nextPageUrl = "null"
			var events  = List[AdEvent]()
			
			for(time <- value){
			  //val ev = time._2.split(",")(4)+"----"+time._2 + "," + nextpageUrl
				val ev = time._2 + "," + nextPageUrl
				val fileds = time._2.split(",")
				val event = AdEvent.apply(fileds(0), fileds(2), fileds(3), fileds(4), fileds(1), nextPageUrl)
        events ::= event
			  nextPageUrl = time._2.split(",")(1)
			}
			events
	}
	
  //sorting RDD to get sequence of user events
	def sortByTime(value : (String, Iterable[(String, String)])) : Map[Long,String] = {

			val ite = value._2.iterator
			
			var events:Map[Long,String] = Map()
			
			while(ite.hasNext){
			  val event = ite.next()
			  val eventval = event._2.replace("[", "").replace("]", "")
			  events += (event._1.toLong -> eventval.toString())
			}
			events = ListMap(events.toSeq.sortWith(_._1 >= _._1):_*)
			events
	}

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
				val rdd = spark.read.json(inputFilePath).rdd

				val filRdd = rdd.filter(_.getAs("visitorId") != null).filter(_.getAs("pageUrl") != null).filter(_.getAs("timestamp") != null)

				println(filRdd.count()+" event lines have visitor Ids.....")
				//mapping and creating RDD visitor id as Key
				val visitorsGroup = filRdd.map(row => (row.getAs("visitorId").toString(), (row.getAs("timestamp").toString(), row.toString()))).groupByKey()

				println(visitorsGroup.count() + " unique visitors.....")
				
				//sorting RDD with timestamp
				val eventJourney = visitorsGroup.map(sortByTime)
				
				//adding nextPageUrl in RDD
				val userEventJourney = eventJourney.flatMap(nextPageUrl)

				//userEventJourney.take(100).foreach(println)
				
			  /*userEventJourney.foreach{r =>
		      println(r)
		    }*/
				
				//saving User Journey in File as JSON String
				userEventJourney.saveAsTextFile(outputFilePath)

				println(s"OutPut is Stored in "+outputFilePath)
				spark.stop()
	}
}
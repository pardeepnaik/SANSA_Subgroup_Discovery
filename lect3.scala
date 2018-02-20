package net.sansa_stack.template.spark.rdf

import java.net.URI
import scala.collection.mutable
import org.apache.spark.sql.SparkSession
import _root_.net.sansa_stack.rdf.spark.io.NTripleReader

object sqlab {

 def main(args: Array[String]) = {
   val input = "src/main/resources/The_Lord_of_the_Rings.ntriples.nt" // args(0)

   val spark = SparkSession.builder
     .master("local[*]")
     .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
     .appName("SparkSQL example")
     .getOrCreate()

   import spark.implicits._

   val tripleDF = spark.sparkContext.textFile(input)
     .map(App.parsTriples)
     .toDF()

   tripleDF.show()

   //tripleDF.collect().foreach(println(_))

   tripleDF.createOrReplaceTempView("triple")

 val sqlText = "SELECT * from triple where subject = 'http://dbpedia.org/resource/The_Lord_of_the_Ring'"
   val triplerelatedtoEvents = spark.sql(sqlText)

   triplerelatedtoEvents.collect.foreach(println)

   val subjectdistribution = spark.sql("select subject, count(*) from triple group by subject")
   println("subjectdistribution:")
   subjectdistribution.show()
   
 }

}

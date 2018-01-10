package net.sansa_stack.template.spark.rdf

import java.net.URI
import scala.collection.mutable
import org.apache.spark.sql.SparkSession
import net.sansa_stack.rdf.spark.io.NTripleReader
import org.apache.spark.ml.feature._
object tfidf{
def main(args: Array[String]) = {
   val input = "src/main/resources/The_Lord_of_the_Rings.ntriples.nt" // args(0)
   println("======================================")
   println("|        Triple reader example 2      |")
   println("======================================")
   val spark = SparkSession.builder
     .master("local[*]")
  .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
     .appName("WordCount example (" + input + ")")
     .getOrCreate()
     
     import spark.implicits._
     
   val triples = spark.sparkContext.textFile(input)
   triples.take(5).foreach(println(_))
   triples.cache()
  
   val nrOfTriples = triples.count()
   println("Count: " + nrOfTriples)

   val removeCommentRows = triples.filter(!_.startsWith("#"))
   removeCommentRows.take(5).foreach(println(_))

   val parsedTriples = removeCommentRows.map(App.parsTriples)
   parsedTriples.take(5).foreach(println(_))
   
   val subject = parsedTriples.map(f => f.subject).toDF("Subject")
   
   val tokenizer = new Tokenizer().setInputCol("Subject").setOutputCol("words")
    val wordsData = tokenizer.transform(subject)

    val hashingTF = new HashingTF()
      .setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(20)

    val featurizedData = hashingTF.transform(wordsData)
    // alternatively, CountVectorizer can also be used to get term frequency vectors

    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)

    val rescaledData = idfModel.transform(featurizedData)
    rescaledData.select("Subject","features").show(false)
    // $example off$

    spark.stop()
}
}
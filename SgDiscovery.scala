////////////////////////////////////////////////
// Authors: Livin Natious, Pardeep Kumar Naik //
// Created on: 01/12/2017                     //
// Version: 0.0.1                             //
// Efficient Subgroup discovery using Spark   //
////////////////////////////////////////////////

package net.sansa_stack.template.spark.rdf

import java.net.URI
import scala.collection.mutable
import scala.collection.immutable.Map
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd.RDD
import net.sansa_stack.rdf.spark.io.NTripleReader
import org.apache.jena.graph.Triple
import org.apache.spark.sql.functions._

object SgDiscovery {
  
  def main(args: Array[String]) = {
    
    //initializing the spark session locally
    val spark = SparkSession.builder
          .master("local[*]")
          .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .appName("SgDiscovery")
          .getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    
    //reading the csv type input dataset
    val dataSetDF = spark.read
         .format("csv")
         .option("header", "true")
         .option("mode", "DROPMALFORMED")
         .option("delimiter", "\t")
         .load("src/main/resources/SG/bank.csv")
      //    dataSetDF.show()
          
    val ontRDD:Array[RDD[Triple]] = new Array[RDD[Triple]](args.length);
    
    //load ontologies into different array elements
    args.zipWithIndex.foreach({
      case(arg, i) => ontRDD(i) = NTripleReader.load(spark, URI.create(arg)).filter(f => {f.getPredicate.toString.contains("subClassOf")})
      })
      
    //print ontology array
    //ontRDD.foreach(x=>if(x!=null){
    //  x.take(5).foreach(x=>println(x.getSubject))
    //  })
 
    val bigRDD = sc.union(ontRDD).distinct
    val dictRDD = bigRDD.map(f => f.getSubject.toString).union(bigRDD.map(f => f.getObject.toString)).distinct
    
    //Create a uri to predicate dictionary data frame dictDF
    val dictDF = dictRDD.map(f=>(f, f.split("#").last)).toDF("uri","predicate")
    //dictDF.show(false)
    
    val ruleInduce = new RuleInduce1(dataSetDF, ontRDD, dictDF, spark)
    
    ruleInduce.run()
    
    //end spark session
    spark.stop
  }
}
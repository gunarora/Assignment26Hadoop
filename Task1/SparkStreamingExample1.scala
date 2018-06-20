package Streaming

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by garora19 on 6/20/2018.
  */
object SparkStreamingExample1 {
  def main(args: Array[String]): Unit = {
    println("hey scala");
    val conf = new SparkConf().setMaster("local[*]").setAppName("simpleApp")
    //println("Elements Count xxxx" + conf);
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    println("Spark Context Created");

    val ssc = new StreamingContext(sc, Seconds(15))
    println("Spark Streaming Context Created");

    val lines = ssc.socketTextStream( hostname = "localhost", port = 9999)
    val words = lines.flatMap(_.split( regex = " "));
    val wordCounts = words.map(x => (x,1)).reduceByKey(_ + _);
    wordCounts.print();
    ssc.start()
    ssc.awaitTermination()

  }
}

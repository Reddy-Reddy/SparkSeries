package com.labs

import org.apache.spark.{SparkConf, SparkContext}

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global
object Async {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("FirstApp1").setMaster("local[*]")
      val sc = new SparkContext(conf)
    val rdd1=sc.parallelize(List(1,2,3,4,5,5))
    val slowrdd1=rdd1.collectAsync().map{x => x.map{x => println("LIST 1 elements"+x);Thread.sleep(3000)}}
    val rdd2=sc.parallelize(List(10,20,30,40,50,50))
    val astrdd2=rdd2.collectAsync().map{x=>x.map{x => println("LIST 2 elements"+x);Thread.sleep(2000)}}

   Await.result(slowrdd1,Duration.Inf)
  }

}

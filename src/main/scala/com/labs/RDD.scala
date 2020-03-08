package com.labs

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object RDD {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("FirstApp").setMaster("local[1]")
    val sc =new SparkContext(conf)

   // val data= Array(1,2,3,4,5,6,7)
    //val parRDD=sc.parallelize(data)
    //parRDD.collect().foreach(println)
    //println(parRDD.reduce(_+_))
    val data = sc.textFile("E:\\LEARNINGs\\workspace\\SparkSeries\\src\\main\\scala\\Datasets\\day1.txt")
    def length1(s :String): Int={
      s.length
    }
    val len=data.map(length1)

    println(len.collect().foreach(println))
    Thread.sleep(10000);


  }
}

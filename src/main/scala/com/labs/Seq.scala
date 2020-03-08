package com.labs

import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.spark.{SparkConf, SparkContext}

object Seq extends App {
  val conf = new SparkConf().setAppName("FirstApp1").setMaster("local[*]")
  val sc = new SparkContext(conf)
  //val data=sc.textFile("E:\\LEARNINGs\\workspace\\SparkSeries\\src\\main\\scala\\Datasets")
  //val seqs=data.map(x=>(NullWritable.get(),x)).saveAsSequenceFile("E:\\LEARNINGs\\workspace\\SparkSeries\\src\\main\\scala\\com\\labs\\seq")
  val data = sc.sequenceFile("E:\\LEARNINGs\\\\workspace\\\\SparkSeries\\\\src\\\\main\\\\scala\\\\com\\\\labs\\\\seq", classOf[NullWritable],
    classOf[Text]).map(r => r.toString()).collect().foreach(println)

}

package com.liubin.wordcount

import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {
    // 建立链接
    val config = new SparkConf().setMaster("local").setAppName("WordCount")
    // 获取上下文
    val sc = new SparkContext(config)
    val tuples = sc.textFile("D:\\tmp\\input\\name.log")
      .flatMap(_.split(" "))
      .map((str: String) => (str, 1))
      .reduceByKey((i: Int, j: Int) => i + j)
      .collect()
    // spark 提供函数
    tuples.foreach(println)
    // 关闭链接
    sc.stop()
  }
}

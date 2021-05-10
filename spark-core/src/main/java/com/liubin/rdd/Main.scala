package com.liubin.rdd

import org.apache.spark.{SparkConf, SparkContext}

object Main {

  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("rdd")
    val sc = new SparkContext(config)
    // 从文件中出创建rdd
    sc.textFile("D:\\tmp\\input\\name.log")
      .flatMap((str: String) => str.split(" "))
      .map((str: String) => (str, 1))
      .reduceByKey((i: Int, i1: Int) => i + i1)
      .foreach(println)
    // 内存中创建rdd
    val seq = Seq[Int](1, 2, 6, 98, 46, 54, 6)
    val rdd = sc.makeRDD(seq)
    val i: Int = rdd.max()
    println(i)
  }
}

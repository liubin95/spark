package com.liubin.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FlatMap {

  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("FlatMap")
    val sc = new SparkContext(config)
    val rdd: RDD[String] = sc.makeRDD(List(
      "hello scala", "hello spark"
    ))
    // map会将结果作为一个数组返回，即出入数据量不变
    rdd.map((str: String) => str.split(" ")).foreach((strings: Array[String]) => println(strings.mkString("Array(", ", ", ")")))
    // flatMap会将元素单独返回，即数据量可以变多
    rdd.flatMap((str: String) => str.split(" ")).foreach((str: String) => println)
  }

}

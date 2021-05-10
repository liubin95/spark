package com.liubin.cache

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CheckPoint {

  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("CheckPoint")
    val sc = new SparkContext(config)
    // 设置检查点文件保存路径
    sc.setCheckpointDir("check-point")
    val line: RDD[String] = sc.makeRDD(List("hello spark", "jvm java", "jvm scala"))
    val mapRdd: RDD[String] = line.flatMap((str: String) => str.split(" "))
    val countRdd: RDD[(String, Int)] = mapRdd.map((str: String) => {
      println("@@@@@@@@@@@@")
      (str, 1)
    })
    // 和cache一起使用，提高效率
    countRdd.cache()
    // 需要设置数据保存路径
    countRdd.checkpoint()

    val reduceRdd: RDD[(String, Int)] = countRdd.reduceByKey(_ + _)
    val result: Array[(String, Int)] = reduceRdd.collect()
    result.foreach(println)
    sc.stop()
  }

}

package com.liubin.acc

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Bc {

  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("Bc")
    val sc = new SparkContext(config)

    val listRdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))
    // 优化join 缓存到内存中，避免笛卡尔积
    val map = mutable.Map(("a", 4), ("b", 5), ("c", 6))
    // 广播变量。不可变
    // 在Executor的jvm内存中多task共享
    val bcMap: Broadcast[mutable.Map[String, Int]] = sc.broadcast(map)
    val mapRdd = listRdd.map((tuple: (String, Int)) => {
      val i = bcMap.value.getOrElse(tuple._1, 0)
      (tuple._1, (tuple._2, i))
    })
    mapRdd.collect().foreach(println)
    sc.stop()
  }

}

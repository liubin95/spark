package com.liubin.cache

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object Persist {

  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("cache")
    val sc = new SparkContext(config)

    val line: RDD[String] = sc.makeRDD(List("hello spark", "hello java", "jvm scala"))
    val mapRdd: RDD[String] = line.flatMap((str: String) => str.split(" "))
    val countRdd: RDD[(String, Int)] = mapRdd.map((str: String) => {
      println("@@@@@@@@@@@@")
      (str, 1)
    })
    // 持久化操作
    // @@@ 不会打印
    // 内存持久化
    countRdd.cache()
    // 磁盘持久化
    countRdd.persist(StorageLevel.DISK_ONLY)


    val reduceRdd: RDD[(String, Int)] = countRdd.reduceByKey(_ + _)
    val result: Array[(String, Int)] = reduceRdd.collect()

    result.foreach(println)
    //RDD 不存储数据，如果对象重用。需要从数据源开始执行
    //@@@@会打印
    val groupRdd: RDD[(String, Iterable[Int])] = countRdd.groupByKey()
    val array: Array[(String, Iterable[Int])] = groupRdd.collect()
    array.foreach(println)

    sc.stop()
  }

}

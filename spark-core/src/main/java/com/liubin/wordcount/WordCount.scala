package com.liubin.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {
    // 建立链接
    val config = new SparkConf().setMaster("local").setAppName("WordCount")
    // 获取上下文
    val sc = new SparkContext(config)
    // 读取文件
    val file: RDD[String] = sc.textFile("D:\\tmp\\input\\name.log")
    println(file.toDebugString)
    println(file.dependencies)
    val word: RDD[String] = file.flatMap(_.split(" "))
    //拆分单词
    val wordMap: RDD[(String, Int)] = word.map((str: String) => (str, 1))
    println(word.toDebugString)
    println(word.dependencies)
    // spark 提供函数
    val reduceByKey = wordMap.reduceByKey((i: Int, j: Int) => i + j, numPartitions = 3)
    val tuples = reduceByKey.collect()
    reduceByKey.foreach(println)
    tuples.foreach(println)
    // 数据分组
    //    val wordGroup: RDD[(String, Iterable[String])] = word.groupBy((str: String) => str)
    //统计数据
    //    val wordCount: Array[(String, Int)] = wordGroup.map {
    //      case (word, list) =>
    //        (word, list.size)
    //    }.collect()
    //    println(wordCount.mkString("Array(", ", ", ")"))

    // 关闭链接
    sc.stop()
  }
}

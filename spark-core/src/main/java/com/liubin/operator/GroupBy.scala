package com.liubin.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GroupBy {

  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("GroupBy")
    val sc = new SparkContext(config)
    // 分组 和分区没有必然关系
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    val value: RDD[(Int, Iterable[Int])] = rdd.groupBy((i: Int) => i % 2)
    val tuples: Array[(Int, Iterable[Int])] = value.collect()
    value.saveAsTextFile("output/group-by")
    println(tuples.mkString("Array(", ", ", ")"))
    val file: RDD[String] = sc.textFile("input/access.log")
    file.map((str: String) => {
      val regex = "\\[.*]".r
      regex findFirstIn str
    }).groupBy((maybeString: Option[String]) => {
      val dataStr = maybeString.get
      dataStr.substring(13, 18)
    }).foreach((tuple: (String, Iterable[Option[String]])) => println(tuple._1 + "---" + tuple._2.size))
  }

}

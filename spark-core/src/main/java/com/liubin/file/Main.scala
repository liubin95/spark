package com.liubin.file

import org.apache.spark.{SparkConf, SparkContext}

object Main {
  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("file")
    val sc = new SparkContext(config)
    // （文件位置，文件内容）
    sc.wholeTextFiles("D:\\tmp\\input\\nginx").collect().foreach(println)
  }

}

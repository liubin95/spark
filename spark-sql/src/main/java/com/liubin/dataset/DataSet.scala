package com.liubin.dataset

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object DataSet {

  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("DataSet")
    val sc = SparkContext.getOrCreate(config)
    val sparkSession = SparkSession.builder().master("local[*]").appName("DataSet").getOrCreate()
    val rdd = sc.makeRDD(List(("zhangsan", 12), ("lisi", 23), ("wangwu", 45)))
    //use case class Person
    case class Person(name: String, age: Int)
    //导入隐饰操作，否则RDD无法调用toDS方法
    import sparkSession.implicits._
    val ds = rdd.toDS()
    ds.printSchema()
    ds.show()
  }

}

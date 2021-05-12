package com.liubin.dataframe

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

object DSL {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").appName("DSL").getOrCreate()
    // 读取文件作为数据源
    val dataFrame = sparkSession.read.json("input/users")
    dataFrame.printSchema()
    // 查询字段
    dataFrame.select("age").show()
    dataFrame.select(dataFrame.col("userName"), dataFrame.col("age") + 1).show()

    // 过滤
    dataFrame.filter(dataFrame.col("age") > 20).show()
    dataFrame.groupBy("age").count().show()

    // dataFrame转换rdd
    val dataToRdd: RDD[Row] = dataFrame.rdd
    dataToRdd.collect().foreach(println)

    // 关闭连接1
    sparkSession.stop()
  }

}

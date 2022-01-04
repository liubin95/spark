package com.liubin.structured.sources

import org.apache.spark.sql.SparkSession

object SocketInput {

  def main(args: Array[String]): Unit = {
    // 创建环境
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("SocketInput")
      .getOrCreate()
    import spark.implicits._
    // 读取数据
    val lines = spark.readStream
      .format("socket")
      .option("host", "172.25.203.166")
      .option("port", 9999)
      .load()
    //处理数据
    val words = lines.as[String].flatMap(_.split(" "))
    val wordCounts = words.groupBy("value").count()

    // 输出结果
    val query = wordCounts.writeStream
      .format("console")
      .outputMode("complete")
      .start()
    // 启动，等待结束
    query.awaitTermination()
    // 关闭资源
    spark.close()
  }

}

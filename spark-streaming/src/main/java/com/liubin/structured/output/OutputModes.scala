package com.liubin.structured.output

import org.apache.spark.sql.SparkSession

object OutputModes {

  def main(args: Array[String]): Unit = {
    // 创建环境
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("TriggerModes")
      .getOrCreate()
    // 读取数据
    val ds = spark.readStream
      .format("socket")
      .option("host", "172.25.203.166")
      .option("port", 9999)
      .load()
    //处理数据
    import spark.implicits._
    val word = ds.as[String].flatMap(_.split(" "))
    word.createOrReplaceTempView("w_count")
    val sql =
      """
        |select value,count(*) as count
        |from w_count
        |group by value
        |""".stripMargin
    val res = spark.sql(sql)

    // 输出模式
    // 1.complete 输出所有数据 - 必须有【聚合】
    // 2.append 输出增加的数据 - 默认 - 只支持简单查询
    // 3.update 输出更新的数据 - 不支持排序
    val query = res.writeStream
      .format("console")
      .outputMode("update")
      .start()
    // 启动，等待结束
    query.awaitTermination()
    // 关闭资源
    spark.close()
  }

}

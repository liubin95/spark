package com.liubin.structured.operations

import org.apache.spark.sql.SparkSession

object SocketOption {

  def main(args: Array[String]): Unit = {
    // 创建环境
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("SocketInput")
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
        |order by count desc
        |""".stripMargin
    val res = spark.sql(sql)

    // 输出结果
    val query = res.writeStream
      .format("console")
      .outputMode("complete")
      .start()
    // 启动，等待结束
    query.awaitTermination()
    // 关闭资源
    spark.close()
  }

}

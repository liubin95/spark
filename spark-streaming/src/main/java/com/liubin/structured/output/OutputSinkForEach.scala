package com.liubin.structured.output

import org.apache.spark.sql.{Dataset, Row, SparkSession}

object OutputSinkForEach {

  def main(args: Array[String]): Unit = {
    // 创建环境
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("OutputSinkForEach")
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

    // 结果1
    res.writeStream
      .outputMode("complete")
      // 每一个批次执行一次
      .foreachBatch((value: Dataset[Row], l: Long) => {
        println(s"batch id $l")
        value.show(false)
      })
      .start().awaitTermination()
    // 关闭资源
    spark.close()
  }

}

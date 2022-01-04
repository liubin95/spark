package com.liubin.structured.output

import org.apache.spark.sql.SparkSession

object OutputSinkMemory {

  def main(args: Array[String]): Unit = {
    // 创建环境
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("OutputSinkMemory")
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
      .format("memory")
      .outputMode("complete")
      .queryName("w_count_res")
      .start()

    while (true) {
      // 结果2
      spark.sql(
        """
          |select *
          |from w_count_res
          |where count > 2
          |""".stripMargin).show()
      Thread.sleep(3000)
    }
    // 关闭资源
    spark.close()
  }

}

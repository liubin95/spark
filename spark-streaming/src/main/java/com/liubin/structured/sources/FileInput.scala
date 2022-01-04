package com.liubin.structured.sources

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object FileInput {

  def main(args: Array[String]): Unit = {
    // 创建环境
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("FileInput")
      .getOrCreate()
    // 指定约束 对于【结构化】数据
    val csvSchema = new StructType()
      .add("年份", IntegerType)
      .add("学校名称", StringType)
      .add("院系名称", StringType)
      .add("专业代码", StringType)
      .add("专业名称", StringType)
      .add("总分", StringType)
      .add("政治__管综", StringType)
      .add("外语", StringType)
      .add("业务课_一", StringType)
      .add("业务课_二", StringType)
    // 读取数据
    val lines = spark.readStream
      .schema(csvSchema)
      .option("header", value = true)
      .csv("input/csv")


    // 输出结果
    val query = lines.writeStream
      .format("console")
      .outputMode("append")
      .option("truncate", value = false)
      .start()
    // 启动，等待结束
    query.awaitTermination()
    // 关闭资源
    spark.close()
  }

}

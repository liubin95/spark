package com.liubin.dataframe

import org.apache.spark.sql.SparkSession

object DataFrame {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").appName("DataFrame").getOrCreate()
    // 读取文件作为数据源
    val dataFrame = sparkSession.read.json("input/info.log")
    dataFrame.show()
    // 创建临时表
    // 不可修改
    // 作用域session
    // dataFrame.createTempView("temp_info")
    // 创建或者替换临时视图
    // dataFrame.createOrReplaceTempView("temp_info")
    // 全局临时表
    // 使用需要加限制【global_temp】
    dataFrame.createGlobalTempView("temp_info")
    // 执行sql，展示结果
    sparkSession.sql("select * from global_temp.temp_info limit 10").show()
    sparkSession.sql("select count(*) from global_temp.temp_info").show()

    // 关闭连接
    sparkSession.stop()
  }

}

package com.liubin.streaming

import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.Date

import com.fasterxml.jackson.annotation.{JsonFormat, JsonIgnoreProperties}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import io.lettuce.core.RedisClient
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

object KafkaLogCount {

  val dateFormat = new SimpleDateFormat("HH:mm")

  def main(args: Array[String]): Unit = {
    // 从检查点恢复数据
    val conf = new SparkConf().setMaster("local[3]").setAppName("KafkaLogCount")
    val ssc = new StreamingContext(conf, Seconds(3))
    ssc.checkpoint("cp")
    // kafka配置信息
    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "LAPTOP-MOJU49C3.localdomain:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "log_group",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer"
    )

    // 卡夫卡工具类，泛型为数据key和value
    val kafkaData: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      // 采集的节点和计算节点的匹配规则
      // 自动选择
      LocationStrategies.PreferConsistent,
      // 订阅信息
      ConsumerStrategies.Subscribe[String, String](Set("dev-log"), kafkaParams)
    )
    val objectMapper = new ObjectMapper()
    val logDataMap = kafkaData.map(msg => {
      objectMapper.registerModule(DefaultScalaModule)
      val str = msg.value()
      objectMapper.readValue(str, classOf[LogData])
    })

    // log 级别分组
    val levelReduce = logDataMap.map(item => (item.level, 1)).reduceByKey(_ + _)
    val levelUpdateState = levelReduce.updateStateByKey((ints, option: Option[Int]) => {
      val i = option.getOrElse(0)
      // 更新到缓存区
      Option(i + ints.sum)
    })
    levelUpdateState.foreachRDD(rdd => {
      rdd.foreachPartition((tuples: Iterator[(String, Int)]) => {
        val redisClient: RedisClient = RedisClient.create("redis://localhost:6379/0")
        val connection = redisClient.connect()
        println("Connected to Redis")
        tuples.foreach((tuple: (String, Int)) => {
          println(tuple)
          connection.sync().hset("hash-level", tuple._1, tuple._2.toString)
        })
        connection.close()
        redisClient.shutdown()
      })
    })

    // log 项目分组
    val serviceReduce = logDataMap.map(item => (item.service_name, 1)).reduceByKey(_ + _)
    val serviceUpdateState = serviceReduce.updateStateByKey((ints, option: Option[Int]) => {
      val i = option.getOrElse(0)
      // 更新到缓存区
      Option(i + ints.sum)
    })
    serviceUpdateState.foreachRDD(rdd => {
      rdd.foreachPartition((tuples: Iterator[(String, Int)]) => {
        val redisClient: RedisClient = RedisClient.create("redis://localhost:6379/0")
        val connection = redisClient.connect()
        println("Connected to Redis")
        tuples.foreach((tuple: (String, Int)) => {
          println(tuple)
          connection.sync().hset("hash-service", tuple._1, tuple._2.toString)
        })
        connection.close()
        redisClient.shutdown()
      })
    })

    // 每分钟log数量
    val timeMap = logDataMap.map(item => {
      val string = item.timestamp.getTime.toString
      val str = string.substring(0, string.length - 3)
      (str.toLong, 1)
    })
    val countWindow = timeMap.reduceByKeyAndWindow((i: Int, j: Int) => i + j, Minutes(1), Minutes(1))
    countWindow.foreachRDD(rdd => {
      rdd.foreachPartition((tuples: Iterator[(Long, Int)]) => {
        val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/value_online_sme?useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&autoReconnect=true&allowMultiQueries=true&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai", "root", "123456")
        val del = conn.prepareStatement("INSERT INTO value_online_sme.log_time (time,count) VALUES (?,?) ")
        tuples.foreach((tuple: (Long, Int)) => {
          del.setString(1, (tuple._1 * 1000).toString)
          del.setString(2, tuple._2.toString)
          del.executeUpdate()
          println(tuple)
        })
        conn.close()
      })
    })

    ssc.start()
    ssc.awaitTermination()

  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  case class LogData(
                      request_id: String,
                      logger: String,
                      position: String,
                      port: Int,
                      stack_trace: String,
                      level: String,
                      host: String,
                      service_host: String,
                      thread: String,
                      @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss", timezone = "GMT+8")
                      timestamp: Date,
                      message: String,
                      service_name: String
                    )

}

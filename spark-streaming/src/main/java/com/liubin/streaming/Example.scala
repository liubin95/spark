package com.liubin.streaming

import java.text.SimpleDateFormat
import java.util.Date

import io.lettuce.core.RedisClient
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.support.ConnectionPoolSupport
import org.apache.commons.pool2.impl.{GenericObjectPool, GenericObjectPoolConfig}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.mutable

object Example {

  val dateFormat = new SimpleDateFormat("yyyy-MM-dd")

  def main(args: Array[String]): Unit = {
    val poolConfig = new GenericObjectPoolConfig[StatefulRedisConnection[String, String]]()
    poolConfig.setMaxTotal(20)
    poolConfig.setMaxIdle(5)
    poolConfig.setTestOnReturn(true)
    poolConfig.setTestWhileIdle(true)
    val redisClient: RedisClient = RedisClient.create("redis://localhost:6379/0")
    val redisPool: GenericObjectPool[StatefulRedisConnection[String, String]] =
      ConnectionPoolSupport.createGenericObjectPool(() => redisClient.connect(), poolConfig)

    // spark 相关
    val conf = new SparkConf().setMaster("local[3]").setAppName("Example")
    val ssc = new StreamingContext(conf, Seconds(3))
    val broadcast: Broadcast[GenericObjectPool[StatefulRedisConnection[String, String]]] = ssc.sparkContext.broadcast(redisPool)

    // kafka配置信息
    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "spark_group",
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
      ConsumerStrategies.Subscribe[String, String](Set("spark"), kafkaParams)
    )

    // 数据DS
    val dataDs = kafkaData.map((value: ConsumerRecord[String, String]) => {
      val str = value.value()
      val strings = str.split(" ")
      AdClickCase(strings(0), strings(1), strings(2), strings(3), strings(4))
    })
    // 是否再黑名单
    val filterDs = dataDs.filter(data => {
      // redis 相关
      val connection = broadcast.value.borrowObject()
      val syncCommands = connection.sync()
      val bool = !syncCommands.sismember("black-set", data.user)
      bool
    })

    // 格式转换
    val mapDs = filterDs.map(data => {
      val st = data.st.toLong
      val date = new Date(st)
      val dateStr = dateFormat.format(date)
      ((dateStr, data.user, data.ad), 1)
    })

    // 聚合本次统计
    val reduceDs = mapDs.reduceByKey(_ + _)
    reduceDs.foreachRDD(rdd => {
      rdd.foreach {
        case ((date, user, ad), sum) =>
          // redis 相关
          val connection = broadcast.value.borrowObject()
          val syncCommands = connection.sync()
          // 获取并聚合原有数据
          val key = f"$date-$user-$ad"
          val exists = syncCommands.hexists("data-map", key)
          var newSum = sum
          if (exists) {
            newSum = syncCommands.hget("data-map", key).toInt + sum
          }
          if (newSum > 100) {
            syncCommands.sadd("black-set", user)
          }
          println(((date, user, ad), sum))
          val map: java.util.Map[String, String] = mutable.HashMap(key -> newSum.toString).asJava
          syncCommands.hmset("data-map", map)
          connection.close()
      }
    })


    ssc.start()
    ssc.awaitTermination()

  }

  case class AdClickCase(st: String, area: String, city: String, user: String, ad: String)

}

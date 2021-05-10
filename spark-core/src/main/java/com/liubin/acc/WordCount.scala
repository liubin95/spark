package com.liubin.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object WordCount {

  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(config)
    val wordCountAccumulator = new WordCountAccumulator()
    sc.register(wordCountAccumulator, "WordCount")
    val line: RDD[String] = sc.makeRDD(List("hello spark", "hello java", "jvm scala"))
    val word = line.flatMap((str: String) => str.split(" "))
    word.foreach((str: String) => {
      wordCountAccumulator.add(str)
    })
    wordCountAccumulator.value.foreach(println)
    sc.stop()
  }

  class WordCountAccumulator extends AccumulatorV2[String, mutable.Map[String, Long]] {

    private var map = mutable.Map[String, Long]()

    override def isZero: Boolean = map.isEmpty

    override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = {
      val accumulator = new WordCountAccumulator()
      accumulator.map = this.map
      accumulator
    }

    override def reset(): Unit = map.clear()

    /**
     * 累加值
     * */
    override def add(v: String): Unit = {
      map.update(v, map.getOrElse(v, 0L) + 1)
    }

    /**
     * 合并两个累加器
     * */
    override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
      val otherMap = other.value
      otherMap.foreach((tuple: (String, Long)) => {
        map.update(tuple._1, map.getOrElse(tuple._1, 0L) + 1)
      })
    }

    override def value: mutable.Map[String, Long] = map
  }

}

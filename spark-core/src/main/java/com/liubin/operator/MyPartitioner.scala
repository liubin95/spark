package com.liubin.operator

import org.apache.spark.Partitioner

class MyPartitioner extends Partitioner {

  def numPartitions: Int = 2

  def getPartition(key: Any): Int = {
    if (key.asInstanceOf[Int] % 2 == 0) 1 else 0
  }
}

/**
 * 聚合函数的区别
 *
 * <p>reduceByKey：将数据两两聚合，第一个不操作，分区内和分区间操作一致
 *
 * <p>foldByKey：将数据两两聚合，第一个和默认值操作，分区内和分区间操作一致
 *
 * <p>aggregateByKey：将数据两两聚合，第一个和默认值操作，分区内和分区间操作可以不一致
 *
 * <p>combineByKey：可以让第一个数据转换结构，分区内和分区间操作可以不一致
 */
package com.liubin.operator;

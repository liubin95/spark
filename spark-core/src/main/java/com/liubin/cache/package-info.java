/**
 * 区别
 *
 * <p>cache:数据在内存中，会增加血缘关系
 *
 * <p>persist:保存为临时文件,效率低。作业结束后，临时文件会删除
 *
 * <p>checkpoint:保存为文件，会独立执行作业即保存的算子会执行两次。和cache一起使用
 *
 * <p>checkpoint：会截断原有的血缘关系，重新建立关系。等同于改变数据源
 */
package com.liubin.cache;

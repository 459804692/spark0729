package com.buwenbuhuo.spark.core.project.app

import com.buwenbuhuo.spark.core.project.acc.CategoryAcc
import com.buwenbuhuo.spark.core.project.bean.{CategoryCountInfo, UserVisitAction}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
 **
 *
 * @author 不温卜火
 *         *
 * @create 2020-07-29 13:21
 **
 *         MyCSDN ：  https://buwenbuhuo.blog.csdn.net/
 *
 */
object CategoryTopApp {
  def calcCatgoryTop10(sc: SparkContext, userVisitActionRDD: RDD[UserVisitAction]): List[CategoryCountInfo] =  {
      // 使用累加器完成3个指标的累加： 点击 下单量 支付量
      val acc: CategoryAcc = new CategoryAcc
      sc.register(acc)
      userVisitActionRDD.foreach(action => acc.add(action))

      /*    // 便利 一行行打印
          acc.value.foreach(println)*/

      // 1. 把一个品类的三个指标封装到一个map中
      val cidActionCountGrouped: Map[String, mutable.Map[(String, String), Long]] = acc.value.groupBy(_._1._1)

      // 2. 转换成 CategoryCountInfo 类型的集合, 方便后续处理
      val categoryCountInfoArray: List[CategoryCountInfo] = cidActionCountGrouped.map {
        case (cid, map) =>
          CategoryCountInfo(cid,
            map.getOrElse((cid, "click"), 0L),
            map.getOrElse((cid, "order"), 0L),
            map.getOrElse((cid, "pay"), 0L)
          )
      }.toList

      // 3. 对数据进行排序取top10
      val result: List[CategoryCountInfo] = categoryCountInfoArray.sortBy(info => (-info.clickCount, -info.orderCount, -info.payCount))
        .take(10)


//      result.foreach(println)
      // 4. 返回top10品类
      result

      // 4. 写到jdbc中

    }
}
/*
利用累加器完成
 */
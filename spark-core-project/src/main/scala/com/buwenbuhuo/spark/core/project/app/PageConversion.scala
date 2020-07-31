package com.buwenbuhuo.spark.core.project.app

import java.text.DecimalFormat

import com.buwenbuhuo.spark.core.project.bean.UserVisitAction
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

/**
 **
 *
 * @author 不温卜火
 *         *
 * @create 2020-07-30 15:19
 **
 *         MyCSDN ：  https://buwenbuhuo.blog.csdn.net/
 *
 */
object PageConversion {
    def statPageConversionRate(sc: SparkContext,
                               userVisitActionRDD: RDD[UserVisitAction],
                               pageString:String): Unit ={
      // 1. 做出来目标跳转流 1，2，3，4，5，6，7
      val pages: Array[String] = pageString.split(",")
      val prePages: Array[String] = pages.take(pages.length -1)
      val postPages: Array[String] = pages.takeRight(pages.length -1)
      val targetPageFlows: Array[String] = prePages.zip(postPages).map {
        case (pre, post) => s"$pre->$post"
      }
/*      // 1.1 把targetPages做广播变量，优化性能
      val targetPageFlowsBC: Broadcast[Array[String]] = sc.broadcast(targetPageFlows)*/
//    println(targetPageFlows.toList)

      // 2. 计算分母，计算需要页面的点击量
      val pageAndCount = userVisitActionRDD
        .filter(action => prePages.contains(action.page_id.toString))
        .map(action => (action.page_id, 1))
        .countByKey()
//      println(pageAndCount)    // 没问题

      // 3. 计算分子
      // 3.1 按照sessionID分组，不能先对需要的页面做过滤，否则会应用调整的逻辑
      val sessionIdGrouped: RDD[(String, Iterable[UserVisitAction])] = userVisitActionRDD.groupBy(_ .session_id)
      val pageFlowsRDD: RDD[String] = sessionIdGrouped.flatMap {
        case (sid, actionIt) =>
          // 每个session的行为做一个按照时间排序
          val actions: List[UserVisitAction] = actionIt.toList.sortBy(_.action_time)
          val preActions: List[UserVisitAction] = actions.take(actions.length -1)
          val postActions: List[UserVisitAction] = actions.takeRight(actions.length -1)
          preActions.zip(postActions).map {
            case (preAction, postAction) => s"${preAction.page_id}->${postAction.page_id}"
          }.filter(flow => targetPageFlows.contains(flow))
//            .filter(flow => targetPageFlowsBC.value.contains(flow)) // 使用广播变量 本人使用有错误
      }
//      pageFlowsRDD.collect.foreach(println)

      // 3.2 聚合
      val pageFlowsAndCount = pageFlowsRDD.map((_, 1)).countByKey()
      // 序列化
      val f: DecimalFormat = new DecimalFormat(".00%")

      // 4. 计算调整率
      val result = pageFlowsAndCount.map {
        // pageAndCount 分母
        // 1-> 2 count/1的点击量
        case (flow, count) =>
          val rate = count.toDouble / pageAndCount(flow.split("->")(0).toLong)
          (flow,f.format(rate))
      }
      println(result)




    }


}

/*
1,2,3,4,5,6,7 计算他们的转换率

1. 想办法做出来跳转流
        “ 1->2 ”，“ 2->3 ”，“ 3->4 ”  ...

2. 计算跳转率
    1 -> 2 调整率
          分子
              “1->2” 跳转流的个数
                    如何计算?
                        1. 保证是同一session才能计算，其实就是按照session进行分组

                        2. 按照时间排序

                        3. RDD[“1->2”,“2->3”,“3->4”] map() reduceByKey
                           RDD[UserVisitAction] map
                           RDD[1,2,3,4,5,6,7,8]
                           如果做跳转流：
                              rdd1 = RDD[1,2,3,4,5,6]
                              rdd2 = RDD[2,3,4,5,6,7,8]
                              rdd3 = rdd1.zip(zip).map(...)
                              过滤出来目标跳转流，然后再聚合


           分母
              页数1的点击数


 */

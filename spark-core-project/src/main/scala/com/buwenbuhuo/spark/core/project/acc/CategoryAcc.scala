package com.buwenbuhuo.spark.core.project.acc

import com.buwenbuhuo.spark.core.project.bean.UserVisitAction
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
  **
  * @author 不温卜火
  *         *
  * @create 2020-07-29 12:16
  **
  *         MyCSDN :https://buwenbuhuo.blog.csdn.net/
  */
// in: UserVisitAction out : Map[(种类，“click”) -> count] (品类，"order") -> (品类，"pay") ,-> count
class CategoryAcc extends AccumulatorV2[UserVisitAction,mutable.Map[(String, String), Long]]{
  self =>   //自身类型
  private val map: mutable.Map[(String, String), Long] = mutable.Map[(String, String), Long]()
  // 判断累加器是否为“零”
  override def isZero: Boolean = map.isEmpty

  // 复制累加器
  override def copy(): AccumulatorV2[UserVisitAction, mutable.Map[(String, String), Long]] = {
    val acc: CategoryAcc = new CategoryAcc
    map.synchronized{
      acc.map ++= map    // 可变集合，不应该直接赋值，应该进行数据的复制
    }
    acc

  }

  // 重置累加器 这个方法调用完之后，isZero必须返回ture
  override def reset(): Unit = map.clear()  // 可变集合应该做一个清楚

  // 分区内累加
  override def add(v: UserVisitAction): Unit = {
    // 分别计算3个指标
    // 对不同的行为做不同的处理 if语句  或 匹配模式
    v match {
      // 点击行为
      case action if action.click_category_id != -1 =>
        // (cid,"click") -> 100
        val key:(String,String) = (action.click_category_id.toString, "click")
        map += key -> (map.getOrElse(key,0L) + 1L)

      // 下单行为  切出来的是字符串"null",不是空null
      case action if action.order_category_ids != "null" =>
        // 切出来这次下单的多个品类
        val cIds: Array[String] = action.order_category_ids.split(",")
        cIds.foreach(cid => {
          val key:(String,String) = (cid,"order")
          map += key -> (map.getOrElse(key,0L) + 1L)
        })

      // 支付行为
      case action if action.pay_category_ids != "null" =>
        // 切出来这次下单的多个品类
        val cIds: Array[String] = action.pay_category_ids.split(",")
        cIds.foreach(cid => {
          val key:(String,String) = (cid,"pay")
          map += key -> (map.getOrElse(key,0L) + 1L)
        })

      // 其他非正常情况，做任何处理
      case  _ =>
    }

  }


  // 分区间的合并
  override def merge(other: AccumulatorV2[UserVisitAction, mutable.Map[(String, String), Long]]): Unit = {
    // 把other中的map合并到this(self)的map中
    // 合并map

    other match {
      case o: CategoryAcc =>
        // 1. 遍历 other的map，然后把变量的导致和self的mao进行相加
        /*      o.map.foreach{
                case ((cid,action),count) =>
                  self.map += (cid,action) -> (self.map.getOrElse((cid,action),0L) + count)
              }*/

        // 2， 对other的map进行折叠，把结果都折叠到self的map中
        //  如果是可变map，则所有的变化都是在原集合中发生变化，最后的值可以不用再一次添加
        //  如果是不可变map，则计算的结果，必须重新赋值给原来的map变量
        self.map ++= o.map.foldLeft(self.map){
          case (map,(cidAction,count)) =>
            map += cidAction -> (map.getOrElse(cidAction,0L) + count)
            map
        }

      case  _=>
        throw new UnsupportedOperationException
    }


  }

  // 最终的返回值
  override def value: mutable.Map[(String, String), Long] = map


}
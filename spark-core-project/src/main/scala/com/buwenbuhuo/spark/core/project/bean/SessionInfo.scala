package com.buwenbuhuo.spark.core.project.bean

/**
 **
@author 不温卜火
 **
 * @create 2020-07-30 12:55
 **
 *         MyCSDN ：  https://buwenbuhuo.blog.csdn.net/
 *
 */
case class SessionInfo(sessionId:String,
                       count: Long) extends Ordered[SessionInfo]{
      // 按照降序排列
      // else if (this.count == that.count) 0  这个不能加，否则会去重
      override def compare(that: SessionInfo): Int =
        if (this.count >that.count) -1

        else 1
}

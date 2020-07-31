package com.buwenbuhuo.spark.core.project.app

import com.buwenbuhuo.spark.core.project.bean.{CategoryCountInfo, UserVisitAction}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  **
*@author 不温卜火
  **
  * @create 2020-07-29 12:18
  **
  *         MyCSDN :https://buwenbuhuo.blog.csdn.net/
  */
object ProjectApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("ProjectAPP").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)

    // 把数据从文件读出来
    val sourceRDD: RDD[String] = sc.textFile("D:/user_visit_action.txt")

    // 把数据封装好（封装到样例类中）
//    sourceRDD.collect.foreach(println)
    val userVisitActionRDD: RDD[UserVisitAction] = sourceRDD.map(line => {
    val fields: Array[String] = line.split("_")
      UserVisitAction(
        fields(0),
        fields(1).toLong,
        fields(2),
        fields(3).toLong,
        fields(4),
        fields(5),
        fields(6).toLong,
        fields(7).toLong,
        fields(8),
        fields(9),
        fields(10),
        fields(11),
        fields(12).toLong)
    })

/*
    //    userVisitActionRDD.collect.foreach(println)
        // 需求1：
         val categoryTop10: List[CategoryCountInfo] = CategoryTopApp.calcCatgoryTop10(sc , userVisitActionRDD)

        // 需求2：top10品类的top10session
        // 1.解决方案1
    //    CategorySessionTopApp.statCategorySessionTop10(sc,categoryTop10,userVisitActionRDD)
        // 2，解决方案2
    //    CategorySessionTopApp.statCategorySessionTop10_2(sc,categoryTop10,userVisitActionRDD)
        // 3，解决方案3
    //    CategorySessionTopApp.statCategorySessionTop10_3(sc,categoryTop10,userVisitActionRDD)
        // 3，解决方案4
        CategorySessionTopApp.statCategorySessionTop10_4(sc,categoryTop10,userVisitActionRDD)
    */
    // 三
    PageConversion.statPageConversionRate(sc,userVisitActionRDD,"1,2,3,4,5,6,7,8")

    // 关闭项目（sc）
    sc.stop()

  }

}

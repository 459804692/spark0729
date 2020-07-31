import java.text.DecimalFormat

/**
 **
@author 不温卜火
 **
 * @create 2020-07-31 10:28
 **
 *         MyCSDN ：  https://buwenbuhuo.blog.csdn.net/
 *
 */
object Test1 {
  def main(args: Array[String]): Unit = {
    // SimpleDataFormat
    val f: DecimalFormat = new DecimalFormat("0000.00")
    println(f.format(math.Pi))
    println(f.format(1))
    println(f.format(100))
    println(f.format(222))
    println(f.format(444444))
  }

}

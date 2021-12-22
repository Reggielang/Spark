package summer.common

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import summer.Uitl.EnvCache

import scala.io.Source

trait TDao {
  def readFile(path:String)={
    //todo 1.获取文件，获取原始数据
    val source = Source.fromFile(EnvCache.get()+path)
    val lines = source.getLines().toList
    source.close()
    lines
  }

  def readFileBySpark(path:String)={
    //todo 1.获取文件，获取原始数据
    EnvCache.get().asInstanceOf[SparkContext].textFile(path)
  }


}

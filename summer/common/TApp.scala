package summer.common

import org.apache.hadoop.mapred.Master
import org.apache.spark.{SparkConf, SparkContext}
import summer.Uitl.EnvCache

trait TApp {

  def execute(master: String="local[*]", appName:String)(op: => Unit):Unit={

    val conf = new SparkConf().setMaster(master).setAppName(appName)
    val sc = new SparkContext(conf)
    EnvCache.put(sc)
        try{
          op
        }catch {
          case e:Exception=>e.printStackTrace()
        }

    sc.stop()
    EnvCache.clear()

  }

}

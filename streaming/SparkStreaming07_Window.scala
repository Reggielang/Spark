package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}


object SparkStreaming07_Window {
  def main(args: Array[String]): Unit = {
    //todo 创建环境对象
    // StreamingContext需要2个参数：第一个参数表示环境配置
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    // 第二个参数表示批量处理的周期(采集周期)
    val ssc = new StreamingContext(sparkConf,Seconds(3))

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)

    val wordToOne: DStream[(String, Int)] = lines.map((_, 1))

    //窗口的范围应该是采集周期的整数倍
    //窗口可以滑动的，默认一个采集周期进行滑动
    //这样的话，可能会出现重复数据的计算，为了避免这种情况的出现，可以改变滑动的步长
    val windowDS: DStream[(String, Int)] = wordToOne.window(Seconds(6),Seconds(6))

    val wordCount: DStream[(String, Int)] = windowDS.reduceByKey(_ + _)
    wordCount.print()

    ssc.start()

    ssc.awaitTermination()
  }
}



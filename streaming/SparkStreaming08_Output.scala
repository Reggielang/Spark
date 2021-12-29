package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}


object SparkStreaming08_Output {
  def main(args: Array[String]): Unit = {
    //todo 创建环境对象
    // StreamingContext需要2个参数：第一个参数表示环境配置
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    // 第二个参数表示批量处理的周期(采集周期)
    val ssc = new StreamingContext(sparkConf,Seconds(3))

    ssc.checkpoint("cp")
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)

    val wordToOne: DStream[(String, Int)] = lines.map((_, 1))

    val windowDS: DStream[(String, Int)] = wordToOne.reduceByKeyAndWindow(
      (x,y)=>{x+y},
      (x,y)=>{x-y},
      Seconds(9),Seconds(3)
    )


//    windowDS.print()

    //foreachRDD不会出现时间戳
    windowDS.foreachRDD(
      rdd=>{

      }
    )

    ssc.start()

    ssc.awaitTermination()
  }
}



package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}


object SparkStreaming09_Resume {
  def main(args: Array[String]): Unit = {

    val ssc: StreamingContext = StreamingContext.getActiveOrCreate("cp", () => {
      val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
      // 第二个参数表示批量处理的周期(采集周期)
      val ssc = new StreamingContext(sparkConf, Seconds(3))

      ssc.checkpoint("cp")
      val lines: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)

      val wordToOne: DStream[(String, Int)] = lines.map((_, 1))

      wordToOne.print()

      ssc
    })

    ssc.checkpoint("cp")

    ssc.start()

    ssc.awaitTermination()
  }
}



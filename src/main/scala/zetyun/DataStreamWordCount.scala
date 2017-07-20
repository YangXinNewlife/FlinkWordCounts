package zetyun

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
/**
  * Created by ryan on 17-7-19.
  */
object DataStreamWordCount {
  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.socketTextStream("192.168.1.81", 9999)
    val counts = text.flatMap { _.toLowerCase.split("\\W+") filter { _.nonEmpty } }   // convert into lower and filter empty value
      .map { (_, 1) }       // put every char in text into (char, 1) format
      .keyBy(0)             // use the ( char, 1) first element hash function
      .timeWindow(Time.seconds(5))  // use the window transformation
      .sum(1) // sum the same key's value

    counts.print

    env.execute("Window Stream WordCount")
  }
}

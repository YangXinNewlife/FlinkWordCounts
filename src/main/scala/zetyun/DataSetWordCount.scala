package zetyun

import org.apache.flink.api.scala.ExecutionEnvironment


/**
  * Created by ryan on 17-7-19.
  */
object DataSetWordCount {
  def main(args: Array[String]): Unit ={
    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    // get input data
    val text = env.fromElements("To be, or not to be,--that is the question:--",
      "Whether 'tis nobler in the mind to suffer", "The slings and arrows of outrageous fortune",
      "Or to take arms against a sea of troubles,")

    val counts = text.flatMap { _.toLowerCase.split("\\W+")}
      .map { (_, 1) }       // put (char, 1) format
      .groupBy(0)           // group by key
      .sum(1)               // sum the every key's number

    // emit result and print result
    counts.print()
  }
}

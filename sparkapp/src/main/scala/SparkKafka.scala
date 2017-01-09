import java.util.Properties
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf

object SparkKafka {
  def main(args: Array[String]) {

   val zkQuorum = "youzy.domain:2181"
   val group = "test-consumer-group"
   val topics = "test"
   val numThreads = 2
   var output="hdfs://10.0.12.114:9000/tmp/spark-log.txt"
   val sparkConf = new SparkConf().setAppName("PrintKafka").setMaster("local[2]")
   val ssc =  new StreamingContext(sparkConf, Seconds(10))
   ssc.checkpoint("checkpoint")
   val topicpMap = topics.split(",").map((_,numThreads.toInt)).toMap
   val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicpMap).map(_._2)
   // lines.print()   
   // lines.saveAsTextFiles(output)
   val line = lines.flatMap(_.split("\n"))
   val words = line.map(_.split("\\|"))
   words.foreachRDD(rdd => {
   println("-------------------------------start print-----------------------------------")
   rdd.foreachPartition(partitionOfRecords => {
        partitionOfRecords.foreach(pair => {
	  val pageNum = pair(0)
          val maxClickTime = pair(1)
          val isLike=pair(3)
          if(isLike.equals("1")){
            var res :String = "页面数: " + pageNum + ",停留时间: " + maxClickTime + ",是否点赞: 是"
            println(res)
          }else{
            var res :String = "页面数: " + pageNum + ",停留时间: " + maxClickTime + ",是否点赞: 否"
			println(res)
		  }
          // val pageNum = pair(0)
          // println(pageNum)
        })
      })
    })
   println("-------------------------------end print-----------------------------------")
   ssc.start()
   ssc.awaitTermination()
  }
}

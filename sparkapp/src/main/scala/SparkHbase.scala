import java.util.Properties
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Put

object Blaher {
  def blah(row: String) {
   val hConf = new HBaseConfiguration()
   val hTable = new HTable(hConf, "student")
   // val thePut = new Put(Bytes.toBytes("key1"))
   val thePut = new Put("key1".getBytes())
   // thePut.add(Bytes.toBytes("info"), Bytes.toBytes(row(0)), Bytes.toBytes(row(0)))
   thePut.add("info".getBytes,row.getBytes,row.getBytes)
   hTable.put(thePut)
  }
}


object SparkHbase {
  def main(args: Array[String]) {

   val zkQuorum = "youzy.domain:2181"
   val group = "test-consumer-group"
   val topics = "my-topic-test"
   val numThreads = 2
   var output="hdfs://10.0.12.104:9000/tmp/spark-log.txt"
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
   rdd.foreachPartition(partitionOfRecords => {
        partitionOfRecords.foreach(pair => {
          val pageNum = pair(0)
	  println(pageNum)
          Blaher.blah(pageNum)
        })
      })
    })
   ssc.start()
   ssc.awaitTermination()
  }
}

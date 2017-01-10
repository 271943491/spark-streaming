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
   // Hbase配置
   val tableName = "student" // 定义表名
   val hbaseConf = HBaseConfiguration.create()
   hbaseConf.set("hbase.zookeeper.quorum", "youzy.domain,youzy2.domain,youzy3.domain")
   hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
   hbaseConf.set("hbase.defaults.for.version.skip", "true")
   val hTable = new HTable(hbaseConf, tableName)
   val thePut = new Put("key1".getBytes())
   thePut.add("info".getBytes,row.getBytes,row.getBytes)
   hTable.setAutoFlush(false, false)
   // 写入数据缓存
   hTable.setWriteBufferSize(3*1024*1024)
   hTable.put(thePut)
   // 提交
   hTable.flushCommits()
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

package dedup

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object dedup {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("dedup").setMaster("spark://localhost:7077").setJars(List("/home/soso/Spark.jar"))
    conf.set("spark.executor.memory", "1g")
    conf.set("spark.cores.max", "2")
    val sc = new SparkContext(conf)

    val data = sc.textFile("hdfs://localhost:9000/user/MapReduceAndSparkData/data/dedup/*", 2)
    data.map (line => (line,1)).reduceByKey(
        (v1,v2)=> v1+v2).map(line => line._1).saveAsTextFile(
            "hdfs://localhost:9000/user/MapReduceAndSparkData/result/Spark/dedup")
  }
}
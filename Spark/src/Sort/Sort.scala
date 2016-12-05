package Sort
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Sort {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("sort").setMaster("spark://localhost:7077").setJars(List("/home/soso/Spark.jar"))
    conf.set("spark.executor.memory", "1g")
    conf.set("spark.cores.max", "2")
    val sc = new SparkContext(conf)

    val data = sc.textFile("hdfs://localhost:9000/user/MapReduceAndSparkData/data/Sort/*")
    var cnt = 0;
    data.coalesce(1).sortBy(line => line.toInt).map { line => cnt = cnt + 1; (cnt, line) }.saveAsTextFile(
      "hdfs://localhost:9000/user/MapReduceAndSparkData/result/Spark/Sort")
  }
}
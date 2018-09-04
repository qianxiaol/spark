package testRDD.transformation

import org.apache.spark.{SparkConf, SparkContext}

object values {
    def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setMaster("local[3]").setAppName("app")
      val sc = new SparkContext(conf)

      val list = List((1,"A"),(2,"B"),(3,"C"),(4,"D"),(5,"E"))

      val rdd = sc.parallelize(list)

      //获取所有的value
      println(rdd.values.collect().toList)
  }
}

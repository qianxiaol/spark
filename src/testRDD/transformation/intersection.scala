package testRDD.transformation

import org.apache.spark.{SparkConf, SparkContext}

object intersection {
    def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setMaster("local[3]").setAppName("app")
      val sc = new SparkContext(conf)

      val rdd1 = sc.parallelize(List(1,2,3,4,5))
      val rdd2 = sc.parallelize(List(2,5,6,8,7))


      //返回两个RDD的交集
      rdd1.intersection(rdd2).foreach(println)


  }
}

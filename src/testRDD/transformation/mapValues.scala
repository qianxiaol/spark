package testRDD.transformation

import org.apache.spark.{SparkConf, SparkContext}

object mapValues {
    def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setMaster("local[3]").setAppName("app")
      val sc = new SparkContext(conf)

      val list = List((1,"1"),(2,"2"),(3,"3"),(4,"4"),(1,"a"))
      val rdd = sc.parallelize(list,2)

      //同基本转换操作中的map，针对[K,V]中的V值进行map操作。
      println(rdd.mapValues(_+"_").collect().toList)

  }
}

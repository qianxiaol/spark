package testRDD.transformation

import org.apache.spark.{SparkConf, SparkContext}

object reduceByKey {
    def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setMaster("local[3]").setAppName("app")
      val sc = new SparkContext(conf)

      val list = List(("book",1),("book",1),("animal",1),("book",1),("animal",1),("animal",1))
      val rdd = sc.parallelize(list)

      //针对RDD中相同的key的元素进行合并。
      println(rdd.reduceByKey(_+_).collect().toList)

  }
}

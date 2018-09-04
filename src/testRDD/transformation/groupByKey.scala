package testRDD.transformation

import org.apache.spark.{SparkConf, SparkContext}

object groupByKey {
    def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setMaster("local[3]").setAppName("app")
      val sc = new SparkContext(conf)

      val list = List(("animal","dog"),("animal","cat"),("book","java"),("book","python"),("book","spark"))

      val rdd = sc.parallelize(list)


      //将Key/Value型的RDD中的元素按照Key值进行汇聚，Key值相同的Value值会合并为一个序列。
      rdd.groupByKey().collect().toList.foreach(println)


  }
}

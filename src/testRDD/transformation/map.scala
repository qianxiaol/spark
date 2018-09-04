package testRDD.transformation

import org.apache.spark.{SparkConf, SparkContext}

object map {
    def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setMaster("local[3]").setAppName("app")
      val sc = new SparkContext(conf)

      val rdd = sc.parallelize(List(1,2,3,4,5))


      //将原来RDD的每个数据项通过map中的用户自定义函数f映射转变为一个新的元素
      println(rdd.map(_*5).collect().toList)

  }
}

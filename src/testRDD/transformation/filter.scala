package testRDD.transformation

import org.apache.spark.{SparkConf, SparkContext}

object filter {
    def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setMaster("local[3]").setAppName("app")
      val sc = new SparkContext(conf)

      val rdd = sc.parallelize(List(1,2,3,4,5))

      //对元素进行过滤，对每个元素应用f函数，返回值为true的元素在RDD中保留，返回为false的将过滤掉
      rdd.filter(_%2==0).collect().toList.foreach(println)

  }
}

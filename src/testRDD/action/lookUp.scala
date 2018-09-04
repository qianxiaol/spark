package testRDD.action

import org.apache.spark.{SparkConf, SparkContext}

object lookUp {
    def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setMaster("local[3]").setAppName("app")
      val sc = new SparkContext(conf)

      val list = List((1,"A"),(2,"B"),(3,"C"),(3,"C"))

      val rdd = sc.parallelize(list,3)


      //lookup用于(K,V)类型的RDD,指定K值，返回RDD中该K对应的所有V值。
      println(rdd.lookup(3))

  }
}

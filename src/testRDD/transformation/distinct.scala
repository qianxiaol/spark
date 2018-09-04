package testRDD.transformation

import org.apache.spark.{SparkConf, SparkContext}

object distinct {
    def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setMaster("local[3]").setAppName("app")
      val sc = new SparkContext(conf)

      val list = List(1,1,2,3,4,5,5,5,8,8,6,7,5,5,55,7,8,8,8,8,8,9)

      val rdd = sc.parallelize(list)


      //将原始RDD中重复出现的元素进行过滤，返回一个新生成的RDD、
      //即原RDD中每个元素在新生成的RDD中只出现一次
      println(rdd.distinct().collect().toList)

  }
}

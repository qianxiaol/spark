package testRDD.transformation

import org.apache.spark.{SparkConf, SparkContext}

object sample {
    def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setMaster("local[3]").setAppName("app")
      val sc = new SparkContext(conf)

      val list = List(1,2,3,4,5,6,7,8,9,10)
//      val rdd = sc.parallelize(list)
//      rdd.sample(true,0.5,System.currentTimeMillis).foreach(println)

      val rdd1 = sc.parallelize(Range(1,100).toList, 4)

      //对RDD进行抽样，其中参数withReplacement为true时表示抽样之后还放回，可以被多次抽样，
      //false表示不放回；fraction表示抽样比例；seed为随机数种子，比如当前时间戳
      rdd1.sample(false, 0.1,System.currentTimeMillis).foreach(println)

  }
}

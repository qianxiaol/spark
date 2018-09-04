package testSparkSql

import org.apache.spark.sql.SparkSession

object SparkSqlExample {
  case class Person(name:String,age:Long)
  def main(args: Array[String]): Unit = {
    //创建SparkSession
    val spark = SparkSession
      .builder()
      .appName("app")
      .master("local[3]")
      .getOrCreate()
    import spark.implicits._
    runBasicDataFrameExample(spark)
    runDatasetCreationExample(spark)
    runInferSchemaExample(spark)
  }
  private def runBasicDataFrameExample(spark: SparkSession): Unit = {
    //json文件
    val ds = spark.read.json("D://测试文档/resources/people.json")
    //ds.show()
    //在对DataSet和DataFlame进行操作的时候都需要这个包进行支持

    //ds.printSchema()//按照树的格式输出模式
//    import spark.implicits._
//    ds.select("name").show()
//    ds.select($"name",$"age"+1).show()
//    ds.filter($"age">21).show()//过滤
    //ds.groupBy("name").count().show()

    //创建一个临时的表
    /*
    ds.createOrReplaceTempView("people")
    val sqlDS = spark.sql("select * from people")
    sqlDS.show()
    */
    //创建全局的表
    /*
    ds.createGlobalTempView("people")
    spark.sql("select * from global_temp.people").show()
    spark.newSession().sql("select * from global_temp.people").show()
    */
  }
  private def runDatasetCreationExample(spark: SparkSession): Unit ={
    import spark.implicits._
    val caseClassDS = Seq(Person("Andy",32)).toDS()
    //caseClassDS.show()

    val primitiveDS = Seq(1,2,3).toDS()
    //primitiveDS.map(_+1).collect().foreach(print)

    val peopleDS = spark.read.json("D://测试文档/resources/people.json").as[Person]
    //peopleDS.show()
  }
  private def runInferSchemaExample(spark:SparkSession): Unit ={
    import spark.implicits._
    val peopleDF = spark.sparkContext
      .textFile("D://测试文档/resources/people.txt")
      .map(_.split(","))
      .map(attr=>Person(attr(0),attr(1).trim.toInt))
      .toDF()
    peopleDF.createOrReplaceTempView("people")
    val teenagerDF = spark.sql("select name,age from people where age between 13 and 19")
    teenagerDF.map(teenager=>"Name:"+teenager(0)).show()
  }
}

package testp

import java.io.StringReader
import java.text.SimpleDateFormat

import au.com.bytecode.opencsv.CSVReader
import netAnalysis.FileUtil
import org.apache.spark.{SparkConf, SparkContext}

object test {
//  def main(args: Array[String]): Unit = {
//    var conf=new SparkConf().setMaster("local[3]").setAppName("app")
//    var sc=new SparkContext(conf)
//    //sc.textFile("/home/user/test_data/test.txt").flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).foreach(println)
//    val info=List(("lxq",List(1,2,2)),("lxq",List(4,5,6)),("cyb",List(1,2,3)),("lxq",List(4,5,6)))
//    val re=sc.parallelize(info)
//    var result=List[(String,List[Int])]()
////    for(i<-0 to 2) result=re.reduceByKey((x,y)=>List(x(i)+y(i))).collect().toList
//    println(result)
//
//  }
//最大熬夜时间
var maxStayUpLateCount:Int = 0
  //最小熬夜时间
  var minStayUpLateCount:Int = 0
  //最大在线时间
  var maxOnlineDuration:Int = 0
  //最小在线时间
  var minOnlineDuration:Int = 0

  //声明文件的字段标题
  val userNetAnalysisFileTitle = "userId,"+"sex,"+
    "onlineYearMonth,"+"semester"

  //生成要返回的数据
  def transformDataList(line:String) = {
    //定义熬夜次数变量
    var stayUpLateCount = 0

    val reader = new CSVReader(new StringReader(line));
    val rn = reader.readNext()
    //学生id
    val userId = rn(0)
    //学生性别
    val sex = rn(1)

    //时间格式化
    val sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm")
    //开始上网时间
    val beginTime = sdf.parse(rn(2))
    //结束上网时间
    val endTime = sdf.parse(rn(3))
    //如果跨夜或者没跨夜但是23:30后下网的 熬夜次数加1
    if(beginTime.getDate != endTime.getDate || (endTime.getHours>=23 && endTime.getMinutes>=30)){
      stayUpLateCount += 1
    }
    //熬夜时长
    val onlineDuration = (endTime.getTime - beginTime.getTime)/3600000
    //把开始时间转成字符串格式，赋予变量 在线的年月
    val onlineYearMonth = new SimpleDateFormat("yyyy/MM").format(beginTime)+"/01"
    //学生入学年份
    val year = userId.take(4).toInt
    //上网的年份
    val onlineYear = onlineYearMonth.take(4).toInt
    //上网的月份
    val onlineMonth = onlineYearMonth.slice(5,7).toInt
    //通过年份和月份计算出学期
    var semester = (onlineYear - year) * 2
    if(onlineMonth > 6){
      semester += 1
    }

    (userId+"#"+sex+"#"+onlineYearMonth+"#"+semester,(stayUpLateCount,onlineDuration.toInt))
  }

  //返回所有字段（把熬夜次数和上网时长展开）
  def transformNetList(s:(String,(Int,Int))) = {
    val para = s._1.split(",").toList
    val userData = para(0).split("#")
    val userId = userData(0)
    val sex = userData(1)
    val onlineYearMonth = userData(2)
    val semester = userData(3)
    val stayUpLateCount = s._2._1
    val onlineDuration = s._2._2

    (userId,sex,stayUpLateCount,onlineDuration,onlineYearMonth,semester)
  }

  //算出本月学生上网能力值
  def transformNetDegree(x:(String,String,Int,Int,String,String)) = {
    val userId = x._1
    val sex = x._2
    val onlineYearMonth = x._5
    val semester = x._6

    //熬夜次数能力值
    val stayUpLateCount = normalize(minStayUpLateCount,maxStayUpLateCount,x._3)
    //在线时长能力值
    val onlineDuration = normalize(minOnlineDuration,maxOnlineDuration,x._4)
    //学生上网程度
    val userNetDegree = 100 * (onlineDuration * 0.7 + stayUpLateCount * 0.3)

    userId+","+sex+","+onlineYearMonth+","+semester+","+userNetDegree
  }

  //能力值算法
  def normalize(minVal:Int,maxVal:Int,x:Int) : Float = {
    if((maxVal.asInstanceOf[Float]-minVal.asInstanceOf[Float]) == 0.0){
      0.toFloat
    }else{
      ((x.asInstanceOf[Float]-minVal.asInstanceOf[Float]) / (maxVal.asInstanceOf[Float]-minVal.asInstanceOf[Float])).toFloat
    }
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("app").setMaster("local[3]")
    val sc = new SparkContext(conf)
    val file = sc.textFile("D:\\测试文档\\washedNetDataFile.csv")
    //生成学号，性别，熬夜次数，上网时长，上网年月，学期字段
    val userNetList = file.map(transformDataList).reduceByKey{(x,y)=>
      (x._1 + y._1,x._2 + y._2)
    }.map(transformNetList)
    //计算出算上网程度所需要的参数
    //最大熬夜时间
    maxStayUpLateCount = userNetList.map(_._3).max()
    //最小熬夜时间
    minStayUpLateCount = userNetList.map(_._3).min()
    //最大在线时间
    maxOnlineDuration = userNetList.map(_._4).max().toInt
    //最小在线时间
    minOnlineDuration = userNetList.map(_._4).min().toInt
    //计算上网程度
    val lines = userNetList.map(transformNetDegree).collect()

    //将分析后的数据写入文件
    for(i <- lines){
      println(i)
    }
  }

}

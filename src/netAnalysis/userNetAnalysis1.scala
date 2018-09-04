package netAnalysis

import java.io.StringReader
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import au.com.bytecode.opencsv.CSVReader
import org.apache.spark.{SparkConf, SparkContext}

object userNetAnalysis1 {
  def transformDataList(line:String) ={
    var count = 0//熬夜次数
    val reader = new CSVReader(new StringReader(line))
    val rn = reader.readNext()
    var userId=rn(0)//id
    var sex=rn(1)//性别
    val sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm")
    val up = sdf.parse(rn(2))//上网时间
    val down=sdf.parse(rn(3))//下网时间
    if(up.getDate!=down.getDate||(down.getHours==23&&down.getMinutes>=30)){
      count +=1
    }
    val onLineTime=(down.getTime-up.getTime)/3600000
    val sdf1=new SimpleDateFormat("yyyy-MM-01")
    val date=new Date()
    val calendar=Calendar.getInstance()
    val yearStart=sdf1.format(date)
    var semester=(calendar.get(Calendar.YEAR)-Integer.parseInt(rn(0).substring(0,4)))*2
    if(date.getMonth>6){
      semester=semester+1
    }
    (userId+"#"+sex+"#"+yearStart+"#"+semester,(count,onLineTime.toInt))
  }
  //将熬夜次数和上网时长展开
  def transformNetList(s:(String,(Int,Int))) ={
    val userInfo=s._1.split("#")
    val userId=userInfo(0)
    val sex=userInfo(1)
    val onlineDate=userInfo(2)
    val semester=userInfo(3)
    val count=s._2._1
    val onlineTime=s._2._2
    (userId,sex,onlineDate,semester,count,onlineTime)
  }
  //计算能力值公式
  def normalize(min:Int,max:Int,x:Int):Float={
    if(max.asInstanceOf[Float]-min.asInstanceOf[Float]==0.0){
      0.toFloat
    }else{
      ((x.asInstanceOf[Float]-min.asInstanceOf[Float])/(max.asInstanceOf[Float]-min.asInstanceOf[Float])).toFloat
    }
  }
  //最小熬夜次数
  var minCount:Int=0
  //最大熬夜次数
  var maxCount:Int=0
  //最小上网时长
  var minTime:Int=0
  //最大上网时长
  var maxTime:Int=0
  //计算学生的能力值
  def transformNetDegree(x:(String,String,String,String,Int,Int))={
    val userId=x._1
    val sex=x._2
    val onlineDate=x._3
    val semester=x._4
    val countVal=normalize(minCount,maxCount,x._5)
    val timeVal=normalize(minTime,maxTime,x._6)
    //上网程度
    val netDegree=100*(timeVal*0.7+countVal*0.3)
    userId+","+sex+","+onlineDate+","+semester+","+netDegree
  }
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName("app")
    val sc = new SparkContext(conf)
    val file = sc.textFile("D:\\测试文档\\washedNetDataFile.csv")
    //生成学号，性别，熬夜次数，上网时长，上网年月，学期
    val userNetList = file.map(transformDataList).reduceByKey{(x,y)=>
      (x._1+y._1,x._2+y._2)
    }.map(transformNetList)
    //最大熬夜次数
    maxCount=userNetList.map(_._5).max()
    //最小熬夜次数
    minCount=userNetList.map(_._5).min()
    //最大在线时间
    maxTime=userNetList.map(_._6).max().toInt
    //最小在线时间
    minTime=userNetList.map(_._6).min().toInt
    //上网程度
    val lines=userNetList.map(transformNetDegree).collect()

    for(i<-lines){
      println(i)
    }
  }
}

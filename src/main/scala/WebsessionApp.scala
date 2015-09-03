
package main.scala

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import scala.collection.mutable.ArrayBuffer
import java.text.SimpleDateFormat

object WebsessionApp {

   def main(args: Array[String]) {
     val conf = new SparkConf().setAppName("WebLog").setMaster("local")
     val sc = new SparkContext(conf)
      println("Starting to process")
      //val apacheLogRegex = """^([\d.]+) (\S+) (\S+) \[([\w\d:/]+\s[+\-]\d{4})\] "(.+?)" (\d{3}) ([\d\-]+) "([^"]+)" "([^"]+)".*""".r
      //val elbLogRegex = """([^ ]*) ([^ ]*) ([^ ]*):([0-9]*) ([^ ]*):([0-9]*) ([.0-9]*) ([.0-9]*) ([.0-9]*) (-|[0-9]*) (-|[0-9]*) ([-0-9]*) ([-0-9]*) \"([^ ]*) ([^ ]*) (- |[^ ]*)\"$""".r
      //val logFile = sc.textFile("/media/alex/LinuxStuff/spark_vagrant/WeblogChallenge/data/2015_07_22_mktplace_shop_web_log_sample.log.gz").cache
      //2015-07-22T09:00:58.369877Z
      val sdf = new SimpleDateFormat("YYYY-MM-dd'T'HH:mm:ss.SSSSSS")
      println(sdf.parse("2015-07-22T09:00:58.369877"))
      val date = """(\d\d\d\d)-(\d\d)-(\d\d)T(\d\d):(\d\d):(\d\d).(\d\d\d\d\d\d)Z""".r
      val logFile = sc.textFile("../WeblogChallenge/data/2015_07_22_mktplace_shop_web_log_sample.log.gz").cache()
      val enTuples = logFile.map(line => line.split(" "))
      val enTimestampIPPairs = enTuples.map(line => (line(2).split(":")(0) , sdf.parse(line(0).replace("Z","")) ))
      //val grouped = enTimestampIPPairs.updateStateByKey(updateValues)
      val groupedIPs = enTimestampIPPairs.groupByKey()
      println(groupedIPs.first())
      //window function lag 
      groupedIPs.foreachPartition( iter => println(iter) )
      //groupedIPs.map(ip => ip._2.collectAsMap())
      //groupedIPs.collect().foreach(ip => println(ip._2.size))
      //groupedIPs.map(ip => println(ip._2.size))
      
//      for( t <- groupedIPs ){
//         //println( "IP : " + t._1 + " number of ts :" + t._2.size )
//         if (t._2.size==1){
//             
//             
//         } else {
//           for(timeStamps <- t._2){
    //             for( ts <- timeStamps){           
    //                 ts match {
    //                    case date(year, month, day, hours,minutes,seconds,milliseconds) => "Good" + seconds
    //                    case x          => "Something else happened" + x 
    //                 }
    //             }
//           }
//           
//         }
//        
//      }
     
      //sc.stop()
   }
}
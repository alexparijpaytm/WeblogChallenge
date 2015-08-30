
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

object WebsessionApp {
   def main(args: Array[String]) {
     val conf = new SparkConf().setAppName("WebLog").setMaster("spark://alex-Latitude-E6510:7077")
     val sc = new SparkContext(conf)
      println("Starting to process")
      //val apacheLogRegex = """^([\d.]+) (\S+) (\S+) \[([\w\d:/]+\s[+\-]\d{4})\] "(.+?)" (\d{3}) ([\d\-]+) "([^"]+)" "([^"]+)".*""".r
      //val elbLogRegex = """([^ ]*) ([^ ]*) ([^ ]*):([0-9]*) ([^ ]*):([0-9]*) ([.0-9]*) ([.0-9]*) ([.0-9]*) (-|[0-9]*) (-|[0-9]*) ([-0-9]*) ([-0-9]*) \"([^ ]*) ([^ ]*) (- |[^ ]*)\"$""".r
      //val logFile = sc.textFile("/media/alex/LinuxStuff/spark_vagrant/WeblogChallenge/data/2015_07_22_mktplace_shop_web_log_sample.log.gz").cache
      //2015-07-22T09:00:58.369877Z
      val date = """(\d\d\d\d)-(\d\d)-(\d\d)T(\d\d):(\d\d):(\d\d).(\d\d\d\d\d\d)Z""".r
      val embeddedDate = date.unanchored
      val logFile = sc.textFile("../WeblogChallenge/data/2015_07_22_mktplace_shop_web_log_sample.log.gz").cache()
      val enTuples = logFile.map(line => line.split(" "))
      val enTimestampIPPairs = enTuples.map(line => (line(2).split(":")(0) , line(0) ))
      
      //var IPSessions:Map[String,Vector[Int]] = Map()
      var sessions = scala.collection.mutable.ListBuffer.empty[Int]
     
      val groupedIPs = enTimestampIPPairs.groupByKey()
      for( t <- groupedIPs ){
         println( "IP : " + t._1 + " number of ts :" + t._2.size )
         for(timeStamps <- t._2){
             if (timeStamps.size==1){
               sessions += 0
             }
             for( ts <- timeStamps){           
                 ts match {
                    case date(year, month, day, hours,minutes,seconds,milliseconds) => "Good" + seconds
                    case x          => "Something else happened" + x 
                 }
             }
         }
      }
      //groupedIPs.count()
      //println(enTimestampIPPairs.count)
      //println(enTuples.first())
      sc.stop()
   }
}
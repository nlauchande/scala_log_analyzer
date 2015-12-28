
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object LogAnalyzerApp {

  def main( args:Array[String]):Unit = {
    val logFile = "log_data_sample.dat"
    val conf = new SparkConf().setMaster("local").setAppName("Log analyzer")
    val sc = new SparkContext(conf)
    val logLines = sc.textFile(logFile)
    
    val accessLogs = logLines.map(line => ApacheAccessLog.parseLogLine(line)).cache()
  
    val contentSizes = accessLogs.map( accessLog => accessLog.contentSize ).cache()
   
   
    val SUM_REDUCER : (Long ,Long) => Long = (_ + _)
    
    val count = contentSizes.count()
    val maxSize = contentSizes.max()
    val minSize = contentSizes.min()
    val average  = contentSizes.reduce(SUM_REDUCER)/count

    //Response code counts
    val pairResponseCode = accessLogs.map( x => (x.responseCode,1) )   
  
   
    println("XXXXXXXXXXX" + accessLogs.count())
    println("XXX : maxSize " + maxSize)  
    println("XXX : count " + count)  
    println("XXX : maxSize " + maxSize)  
    println("XXX : minSize " + minSize) 
    println("XXX : average " + average)  
 
    sc.stop()
  }

}

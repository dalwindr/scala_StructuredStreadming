package product_via_kafka
//package xmltokafka.xml
import java.text.SimpleDateFormat
import java.time.LocalDate
import java.util.{Date, Properties}
//import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.slf4j.LoggerFactory
import java.text.DecimalFormat;
import org.apache.spark.sql.functions.to_timestamp;

/**
  * Created by root on Nov/30/2019.
  */

class MyJsonParser(jsonTemplate:String, json_path:String) {

  private val Events = Array("Event1","Event2","Event3","UNKNOWN")
  private val sources = Array(".com","t.com","m.com","unknown")
  private val products = Array(1210,1211,1212,1213,1214,1215,1216,1217,1218,1219,1220)
  private val customers = Array("CUST-10378903","CUST-7846392","CUST-8801145","CUST-8405512","CUST-11280592","CUST-4914494","CUST-2102862","CUST-419036","CUST-8704868","CUST-11092208","CUST-11092209")
  val random = new scala.util.Random
  var jsonTemplate1= "/Users/keeratjohar2305/Downloads/eclipseSbtSpark/SPARK_KAFKA/templates/SampleJsonFormat.json"
  private var template = scala.io.Source.fromFile(jsonTemplate1).mkString
  private val logger = LoggerFactory.getLogger(getClass)

  def getJson() ={
    
   var jsonData =template.split("\n").map{ x=> 
                                var data = if ( x.length>2 && (x.split(":")).length> 1 ) 
                                                { x.split(":")(1).toString }
                                            else if (x.length>1) 
                                                {x} 
                                
                                var randomNum = data match {
                                case "##RANDOM_TIMESTAMP##" => RandomJsonInt(data.toString)
                                case "##RANDOM_CUSTID##" => RandomJsonString(data.toString)
                                case "##RANDOM_EVENTTYPE##" => RandomJsonString(data.toString)
                                case "##RANDOM_COOKIES##" => RandomJsonString(data.toString)
                                case "##RANDOM_SOURCE##" => RandomJsonString(data.toString)
                                case "##RANDOM_PRODUCT_ID##" => RandomJsonInt(data.toString)
                
                                case "##RANDOM_PRICE##" => RandomJsonDouble(data.toString)
                                case "##RANDOM_QUATITY##" => RandomJsonInt(data.toString)
                
                                case "{" => "{"
                                case "[" => "["
                                case "[{" => "[{"
                                case "}," => "},"
                                case "}]," => "}],"

                                case _=> None
                                }
                               println(data.toString , "   ......to......  ", randomNum.toString)
                              (x.toString.replace(data.toString,randomNum.toString))
                              }.mkString("\n") 
      jsonData
  }
  def start(): Unit ={

    // val properties = new Properties()
    // comma separated list of Kafka brokers
    // properties.setProperty("bootstrap.servers", s"${conf.broker}:${conf.port}")
    // properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    // properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    // properties.put("key-class-type", "java.lang.String")
    // properties.put("value-class-type", "java.lang.String")

    // val producer = new KafkaProducer[String, String](properties)
    // val date = raw"(\d{4})-(\d{2})-(\d{2})".r
    // val dates = "Important dates in history: 2004-01-20, 1958-09-05, 2010-10-06, 2011-07-15"
    // date.findAllIn(dates)
          var i=0
          val frequency=1
          var delay = 30
          while ( i != frequency)
                    { 
                    val jsonData = getJson
                    
                                  //logger.info(message)
                    i = i + 1
                    Thread.sleep(delay.toInt)
                    println(jsonData)
                  }
    }
    

    def RandomJsonInt(x: String)= { 
                                      var      dayid = random.nextInt(28)   + 1;
                                      var      monthid =  random.nextInt(11) + 1; 
                                       var     yearid = 2019;
                                        var    hoursid =  random.nextInt(24) + 1;
                                        var    Minutesid =  random.nextInt(58) + 1;
                                         var   Secondsid =  random.nextInt(58) + 1;
                                    
                                    if (x == "##RANDOM_PRODUCT_ID##" )
                                              products(random.nextInt(9)) + ","
                                    else if (x == "##RANDOM_TIMESTAMP##")
                                    { 
                                         '"' + yearid.toString() + '/' + monthid.toString() + '/' + dayid.toString() + ' ' + hoursid.toString() + ':' +  Minutesid.toString() +':' + Secondsid.toString() +  '"' + ","
                                    }      
                                    else if ( x == "##RANDOM_QUATITY##" )
                                            random.nextInt(20).toString + ","
                                         
                                  }

    def  RandomJsonString(x: String)= {
                            if (x == "##RANDOM_EVENTTYPE##" )
                                 '"' +  Events(random.nextInt(3)) +  '"' + ","
                            else if (x == "##RANDOM_COOKIES##" )
                              '"' +   "COOKIES-" + random.alphanumeric.take(2).mkString("*#1232$%^_6&@%^&") + '"' 
                            else if (x == "##RANDOM_SOURCE##" )
                                  '"' + sources(random.nextInt(4)) + '"' + ","
                            else if (x == "##RANDOM_CUSTID##")
                                        '"' + customers(random.nextInt(9)) + '"' + ","
                                }
      def RandomJsonDouble(x: String)= {  
                                   
                                    val df = new DecimalFormat("0.00")
                                    if (x == "##RANDOM_PRICE##" )
                                              df.format(random.nextDouble()*1000)

                                  }
    
}
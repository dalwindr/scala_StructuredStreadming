package product_via_kafka

//spark-submit --class StructStreamingConsumer --master local[*] --driver-class-path /Users/keeratjohar2305/.ivy2/cache/org.apache.kafka/kafka-clients/jars/kafka-clients-2.1.1.jar:/Users/keeratjohar2305/.ivy2/cache/org.apache.spark/spark-sql-kafka-0-10_2.11/jars/spark-sql-kafka-0-10_2.11-2.4.0.jar /Users/keeratjohar2305/Downloads/SPARK_POC/target/scala-2.11/spark_poc_2.11-0.1.jar

import org.apache.spark.sql.streaming.Trigger
import scala.concurrent.duration._
import org.apache.spark.sql.functions.to_timestamp
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.functions.udf
import org.apache.log4j._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener._

object sparkConsumerForKafka{
	Logger.getLogger("org").setLevel(Level.ERROR)
	def main(args: Array[String]) {

   val spark = SparkSession
   .builder
   .appName("Spark-Kafka-Integration")
   .config("spark.sql.warehouse.dir", "/Users/keeratjohar2305/Downloads/eclipseSbtSpark/SPARK_KAFKA/spark-warehouse")
   .master("local")
   .getOrCreate()
  import spark.implicits._ 

  val df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").
                option("subscribe", "demo_test").
                //option("startingOffsets", "earliest").
                option("max.poll.records", 10).
                //option("failOnDataLoss", false).
                load()

    val sparkSchema1 = new StructType().
                    add("timestamp",StringType).
                    add("customerid", StringType).
                    add("eventType", StringType).
                    add("productview", new StructType().add("product id",StringType).add("price",StringType)).
                    add("addtocard", new StructType().add("product id",StringType).add("price",StringType)).
                    add("purchase", StringType). 
                    add("source", StringType).
                    add("cookieid", StringType)
                    
    val prop=new java.util.Properties()
    prop.put("user","testabc")
    prop.put("password","testabc")
    val url="jdbc:mysql://localhost:3306/testabc"
  
    val cust_master =spark.read.jdbc(url,"customer_master",prop) 
    val prod_master =spark.read.jdbc(url,"product_master",prop) 
    println("Count of Customer Master: ",cust_master.count())
    println("Count of Product Master: ",prod_master.count())
    
   
   val df2 = df.selectExpr("cast (value as string) as json").
               select(from_json(col("json").cast("string"), sparkSchema1).alias("value")).select("value.*")

   df2.createOrReplaceTempView("EventTable")
   
   val df3 = spark.sql("select timestamp,customerid,eventType,productview.*,addtocard.*,purchase,source,cookieid from EventTable").
                 toDF("timestamp","customerid","eventType", "pv_product_id","pv_price", "A2C_product_id","A2C_quantity","purchase","source","cookieid" )
  
  df3.createOrReplaceTempView("EventTable3")
  cust_master.createOrReplaceTempView("customer_master")
  prod_master.createOrReplaceTempView("product_master")
  
  spark.sql("desc table EventTable3").foreach(x=> println(x))
  println("\n")
  spark.sql("desc table customer_master").foreach(x=> println(x))
   println("\n")
  spark.sql("desc table product_master").foreach(x=> println(x))
   println("\n")
    
   
   val df4 = spark.sql("""select customerid,pm.product_id,pm.category,pm.subcategory,
                             case when (length(cast(cm.zipcode as varchar(6))) < 6 and length(cast(cm.zipcode as varchar(6))) == 4)
                                              then cast(cm.state_code as varchar(2))||cast(cm.zipcode as varchar(4))
                                              
                                  when length(cast(cm.zipcode as varchar(6))) < 6 and length(cast(cm.zipcode as varchar(6)))  == 3
                                            then cast(cm.state_code as varchar(2))||cast(cm.city_code as varchar(1))||cast(cm.zipcode as varchar(3))
       
                                  else 
                                       cast(cm.zipcode as varchar(6)) end as zipcode2,   
                          
                         cm.city,cm.state,
                         
                        
                         
                         case when source == 'm.com' then 'Mobile'
                              when source == 't.com' then 'Tablet'
                              when source == '.com' then 'Desktop'
                              else 'unknown_channel' end as channel,
                          hour(to_timestamp(et.timestamp,'yyyy/MM/dd HH:mm:ss')) as hour
                          from 
                          customer_master cm, EventTable3 et,product_master pm where 
                          cm.cust_id = et.customerid and pm.product_id = et.pv_product_id
                          and et.eventType != 'UNKNOWN'
                          """)
  
             
  println("This is dalwinder singh",df4.dtypes.mkString("\n"))
  println(spark.conf.get("spark.sql.warehouse.dir"))
  println(spark.catalog.listTables.show(false))
 
 
   val streamListener = new StreamingQueryListener() {
    override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = {
        println("Query started:" + queryStarted.id)
    }
    override def onQueryTerminated(queryTerminated: QueryTerminatedEvent): Unit = {
        println("Query terminated" + queryTerminated.id)
    }
    override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {
     println("Query made progress")
        println("Starting offset:" + queryProgress.progress.sources(0).startOffset)
        println("Ending offset:" + queryProgress.progress.sources(0).endOffset)
        //Logic to save these offsets
    }
}
  
spark.streams.addListener(streamListener)
  
  //Micro-Batch Stream Processing( Trigger.Once and Trigger.ProcessingTime triggers)
 var s = df4
      .writeStream
    //option("truncate", "false").
    //option("failOnDataLoss", false).
    //format("console")
      .queryName("kafka2console-microbatch")
    //trigger(Trigger.ProcessingTime(30.seconds)).start()
    .format("parquet")
    .option("path", "/Users/keeratjohar2305/Downloads/eclipseSbtSpark/SPARK_KAFKA/spark-warehouse/product_catalog")
    .option("checkpointLocation", "/Users/keeratjohar2305/Downloads/eclipseSbtSpark/check")
    .option("startingOffsets",  """ {"articleA":{"0":23,"1":-1},"articleB":{"0":-2}} """)
    .partitionBy("hour").outputMode("Append")
    .trigger(Trigger.ProcessingTime("25 seconds")).start()
                      
//s.start()
//s.stop()
s.awaitTermination()




//Continuous Stream Processing (Trigger.Continuous trigger) via KafkaContinuousReader)
//val s2 = df1.writeStream.option("failOnDataLoss", false).format("console").queryName("kafka2console-contineous").trigger(Trigger.Continuous(120.seconds)).option("checkpointLocation", "/Users/keeratjohar2305/Downloads/SPARK_STRUCK_STREAM_WAL/EXP1").start()

//s2.awaitTermination
//df.isStreaming
//df.printSchema
//df.count()


}
}
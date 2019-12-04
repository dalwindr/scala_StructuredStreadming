package product_via_kafka
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.util.concurrent.Future
import org.apache.kafka.clients.producer.RecordMetadata

object KafkaProducerApp extends App {

  val topic = util.Try(args(0)).getOrElse("demo_test")
  println(s"Connecting to $topic")
 
  val props = new java.util.Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("client.id", "KafkaProducer")
  props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[Integer, String](props)
  
  while (true)
   {
    Thread.sleep(1000)
    for (j <- 1 until  50 ) {
    val polish = java.time.format.DateTimeFormatter.ofPattern("dd.MM.yyyy H:mm:ss")
    val now = java.time.LocalDateTime.now().format(polish)
 
    val record = new ProducerRecord[Integer, String](topic, 1, new MyJsonParser( "","").getJson())
    val metaF: Future[RecordMetadata] = producer.send(record)
   
    val meta = metaF.get() // blocking!
    val msgLog =
      s"""
         |offset    = ${meta.offset()}
         |partition = ${meta.partition()}
         |topic     = ${meta.topic()}
       """.stripMargin
    println(msgLog)
  }
}
  producer.close()

  /*

     for(int i = 0; i < 10; i++)
         producer.send(new ProducerRecord<String, String>(topicName, 
            Integer.toString(i), Integer.toString(i)));
               System.out.println(“Message sent successfully”);
               producer.close();

  */

}

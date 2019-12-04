package product_via_kafka

//import product_via_kafka.MyJsonParser
/**
  * Created by root on 7/28/17.
  */

object JsonGenerator extends App {

    println("-------------------------");
    println("  XMLKafka Generator  ");
    println("template.xml: " +  args(0));
    println("application.conf: " + args(1));
    println("-------------------------");

    def checkExtFiles(ext:String, filename:String): Boolean ={
        val pat = s"""(.*)[.](${ext})""".r
    
        filename match {
          case pat(fn,ex) => true
          case _ => false
        }
    }

    if (args.length == 2 && checkExtFiles("json",args(0)) && checkExtFiles("conf",args(1)))
          new MyJsonParser(args(0),args(1)).start();
    else
          { println("Usage: java -jar XMLKafkaGenerator-v1.0-SNAPSHOT.jar <template.xml> <application.conf>")
                println("""cd /Users/keeratjohar2305/Downloads/SPARK_POC ;sbt "runMain xmltokafka.XmlProducer conf/example.xml  conf/application.conf" """)
          }
}

/*

var ext = "conf/example.xml"
...............
 2 match {  case 1 => "one"
      case 2 => "two"
      case _ => "many"
   }

 1 match {  case 1 => "one"
      case 2 => "two"
      case _ => "many"
   }
------------------------
case class Person(name: String, age: Int)
      val alice = new Person("Alice", 25)
      val bob = new Person("Bob", 32)
      val charlie = new Person("Charlie", 32)
      for (p <- List(alice, bob, charlie)) {println(p)}

      for (p <- List(alice, bob, charlie)) {
        p match {
          case Person("Alice", 25) => println("Hi , Alice")
          case Person("Bob", 32) => println("Hi , Bob")
          case Person("Charlie", 32) => println("Hi , Charlie")

        }
      }


      val pattern = "Scala".r
      val str = "Scala is Scalable and cool"
      println(pattern findFirstIn str)

      val str = "I am cool"
      println(pattern findFirstIn str)


      val pattern = "(S|s)cala".r
      val str = "Scala is scalable and cool , but supercoolScala"
      println((pattern findAllIn str).mkString(","))
      println(pattern replaceFirstIn(str, "java"))
      println(pattern replaceAllIn(str, "java"))


val date = raw"(\d{4})-(\d{2})-(\d{2})".r
"2004-01-20" match {
  case date(year, month, day) => s"$year was a good year for PLs."
}

val dates = "Important dates in history: 2004-01-20, 1958-09-05, 2010-10-06, 2011-07-15"
val firstDate = date.findFirstIn(dates).getOrElse("No date found.")

val firstDate = date.replaceFirstIn(dates,9999-29-09).getOrElse("No date found.")
val firstDate = date.replaceFirstIn(dates,"9999-29-09")


  */
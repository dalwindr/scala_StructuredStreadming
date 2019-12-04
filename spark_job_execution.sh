spark-submit --class sparkConsumerForKafka --master local[*] --driver-class-path /Users/keeratjohar2305/.ivy2/cache/org.apache.kafka/kafka-clients/jars/kafka-clients-2.1.1.jar:/Users/keeratjohar2305/.ivy2/cache/org.apache.spark/spark-sql-kafka-0-10_2.11/jars/spark-sql-kafka-0-10_2.11-2.4.0.jar /Users/keeratjohar2305/Downloads/eclipseSbtSpark/SPARK_KAFKA/target/scala-2.11/spark_poc_2.11-0.1.jar

spark-submit --class resultAndQueries --master local[*]  /Users/keeratjohar2305/Downloads/eclipseSbtSpark/SPARK_KAFKA/target/scala-2.11/spark_poc_2.11-0.1.jar

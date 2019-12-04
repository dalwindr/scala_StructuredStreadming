# scala_StructuredStreadming

1) Json generator generates json files, 
2) producers push to kafka, 
3) spark structured streaming consumer consumes data from kafka

Note: Kafka , mysql, spark is configured locally on mac

# Created Following two table in MySQL DB

1) create table product_master(product_id STRING, category STRING, subcategory STRING)
2) create table customer_master(cust_id varchar(100), custname varchar(100), city varchar(100) , state varchar(100), state_code Integer,
city_code integer, zipcode Integer) ;

# Load test data
3)
insert into customer_master values('CUST-10378903','Sabby','KARNATAKA','Banglore',56,5,565018);
insert into customer_master values('CUST-7846392','John','DELHI','New Delhi',11,0,110018);
insert into customer_master values('CUST-8801145','Yuri','MAHARASHTRA','Pune',40,1,017 );
insert into customer_master values('CUST-8405512','Eugene','TAMIL NADU','Chennai',64,5,119);
insert into customer_master values('CUST-11280592','Yusin','DELHI','New Delhi',11,0,110018);
insert into customer_master values('CUST-4914494','Romon','KARNATAKA','Banglore',56,5,565018);
insert into customer_master values('CUST-2102862','Bhavin','TAMIL NADU','Chennai',64,5,119);
insert into customer_master values('CUST-419036','Bhavana','DELHI','New Delhi',11,0,110018);
insert into customer_master values('CUST-8704868','Rajnesh','MAHARASHTRA','Pune',40,1,017);
insert into customer_master values('CUST-11092208','Kovind','DELHI','New Delhi',11,0,110018);
insert into customer_master values('CUST-11092209','Modi','MARHARASHTRA','Banglore',56,5,565018);


4)
insert into product_master values('1210','cat1','subcat1');
insert into product_master values('1211','cat1','subcat2');
insert into product_master values('1212','cat1','subcat3');
insert into product_master values('1213','cat2','subcat11');
insert into product_master values('1214','cat2','subcat2');
insert into product_master values('1215','cat2','subcat13');
insert into product_master values('1216','cat3','subcat13');
insert into product_master values('1217','cat3','subcat24');
insert into product_master values('1218','cat3','subcat26');
insert into product_master values('1219','cat4','subcat27');
insert into product_master values('1220','cat4','subcat28');


# 5) To load data in kafka topic
Producer utility created to produce Json files to kafka topic

sbt "runMain product_via_kafka.KafkaProducerApp"

# 6) submit the spark job
Spark Structured streamer will consumer json from the kafka topic

spark-submit --class sparkConsumerForKafka --master local[*] --driver-class-path /Users/keeratjohar2305/.ivy2/cache/org.apache.kafka/kafka-clients/jars/kafka-clients-2.1.1.jar:/Users/keeratjohar2305/.ivy2/cache/org.apache.spark/spark-sql-kafka-0-10_2.11/jars/spark-sql-kafka-0-10_2.11-2.4.0.jar /Users/keeratjohar2305/Downloads/eclipseSbtSpark/SPARK_KAFKA/target/scala-2.11/spark_poc_2.11-0.1.jar

# 7) get the result for asked queries.
This will generates 
Top 5 selling product from given channel
Top 5 selling product from given category
total sold products by zipcode,city,state


spark-submit --class resultAndQueries --master local[*]  /Users/keeratjohar2305/Downloads/eclipseSbtSpark/SPARK_KAFKA/target/scala-2.11/spark_poc_2.11-0.1.jar



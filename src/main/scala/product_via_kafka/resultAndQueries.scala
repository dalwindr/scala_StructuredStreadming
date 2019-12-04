package product_via_kafka
import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.col

object resultAndQueries {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.FATAL)
   
    val spark = SparkSession
   .builder
   .appName("Spark-Kafka-Integration")
   .master("local")
   .enableHiveSupport()
   .getOrCreate()
   
  import spark.implicits._  
    
  
 spark.sql("""create external table IF NOT EXISTS product_catalog (
product_id String ,
category String,
subcategory String,
zipcode2 String,
city String,
state String,
channel String
)
PARTITIONED by (hour Integer )
stored as PARQUET
location '/Users/keeratjohar2305/Downloads/eclipseSbtSpark/SPARK_KAFKA/spark-warehouse/product_catalog'""")
  
spark.sql("""ALTER TABLE product_catalog ADD IF NOT EXISTS
    PARTITION (hour=1) LOCATION '/Users/keeratjohar2305/Downloads/eclipseSbtSpark/SPARK_KAFKA/spark-warehouse/product_catalog/hour=1'""")
spark.sql("""ALTER TABLE product_catalog ADD IF NOT EXISTS
    PARTITION (hour=2) LOCATION '/Users/keeratjohar2305/Downloads/eclipseSbtSpark/SPARK_KAFKA/spark-warehouse/product_catalog/hour=2'""")
spark.sql("""ALTER TABLE product_catalog ADD IF NOT EXISTS
    PARTITION (hour=3) LOCATION '/Users/keeratjohar2305/Downloads/eclipseSbtSpark/SPARK_KAFKA/spark-warehouse/product_catalog/hour=3'""")
spark.sql("""ALTER TABLE product_catalog ADD IF NOT EXISTS
    PARTITION (hour=4) LOCATION '/Users/keeratjohar2305/Downloads/eclipseSbtSpark/SPARK_KAFKA/spark-warehouse/product_catalog/hour=4'""")
spark.sql("""ALTER TABLE product_catalog ADD IF NOT EXISTS
    PARTITION (hour=5) LOCATION '/Users/keeratjohar2305/Downloads/eclipseSbtSpark/SPARK_KAFKA/spark-warehouse/product_catalog/hour=5'""")
spark.sql("""ALTER TABLE product_catalog ADD IF NOT EXISTS
    PARTITION (hour=6) LOCATION '/Users/keeratjohar2305/Downloads/eclipseSbtSpark/SPARK_KAFKA/spark-warehouse/product_catalog/hour=6'""")

spark.sql("""ALTER TABLE product_catalog ADD IF NOT EXISTS
    PARTITION (hour=7) LOCATION '/Users/keeratjohar2305/Downloads/eclipseSbtSpark/SPARK_KAFKA/spark-warehouse/product_catalog/hour=7'""")
spark.sql("""ALTER TABLE product_catalog ADD IF NOT EXISTS
    PARTITION (hour=8) LOCATION '/Users/keeratjohar2305/Downloads/eclipseSbtSpark/SPARK_KAFKA/spark-warehouse/product_catalog/hour=8'""")
spark.sql("""ALTER TABLE product_catalog ADD IF NOT EXISTS
    PARTITION (hour=9) LOCATION '/Users/keeratjohar2305/Downloads/eclipseSbtSpark/SPARK_KAFKA/spark-warehouse/product_catalog/hour=9'""")
spark.sql("""ALTER TABLE product_catalog ADD IF NOT EXISTS
    PARTITION (hour=10) LOCATION '/Users/keeratjohar2305/Downloads/eclipseSbtSpark/SPARK_KAFKA/spark-warehouse/product_catalog/hour=10'""")
spark.sql("""ALTER TABLE product_catalog ADD IF NOT EXISTS
    PARTITION (hour=11) LOCATION '/Users/keeratjohar2305/Downloads/eclipseSbtSpark/SPARK_KAFKA/spark-warehouse/product_catalog/hour=11'""")
spark.sql("""ALTER TABLE product_catalog ADD IF NOT EXISTS
    PARTITION (hour=12) LOCATION '/Users/keeratjohar2305/Downloads/eclipseSbtSpark/SPARK_KAFKA/spark-warehouse/product_catalog/hour=12'""")

spark.sql("""ALTER TABLE product_catalog ADD IF NOT EXISTS
    PARTITION (hour=13) LOCATION '/Users/keeratjohar2305/Downloads/eclipseSbtSpark/SPARK_KAFKA/spark-warehouse/product_catalog/hour=13'""")
spark.sql("""ALTER TABLE product_catalog ADD IF NOT EXISTS
    PARTITION (hour=14) LOCATION '/Users/keeratjohar2305/Downloads/eclipseSbtSpark/SPARK_KAFKA/spark-warehouse/product_catalog/hour=14'""")
spark.sql("""ALTER TABLE product_catalog ADD IF NOT EXISTS
    PARTITION (hour=15) LOCATION '/Users/keeratjohar2305/Downloads/eclipseSbtSpark/SPARK_KAFKA/spark-warehouse/product_catalog/hour=15'""")
spark.sql("""ALTER TABLE product_catalog ADD IF NOT EXISTS
    PARTITION (hour=16) LOCATION '/Users/keeratjohar2305/Downloads/eclipseSbtSpark/SPARK_KAFKA/spark-warehouse/product_catalog/hour=16'""")
spark.sql("""ALTER TABLE product_catalog ADD IF NOT EXISTS
    PARTITION (hour=17) LOCATION '/Users/keeratjohar2305/Downloads/eclipseSbtSpark/SPARK_KAFKA/spark-warehouse/product_catalog/hour=17'""")
spark.sql("""ALTER TABLE product_catalog ADD IF NOT EXISTS
    PARTITION (hour=18) LOCATION '/Users/keeratjohar2305/Downloads/eclipseSbtSpark/SPARK_KAFKA/spark-warehouse/product_catalog/hour=18'""")
 
 spark.sql("""ALTER TABLE product_catalog ADD IF NOT EXISTS
    PARTITION (hour=19) LOCATION '/Users/keeratjohar2305/Downloads/eclipseSbtSpark/SPARK_KAFKA/spark-warehouse/product_catalog/hour=19'""")
spark.sql("""ALTER TABLE product_catalog ADD IF NOT EXISTS
    PARTITION (hour=20) LOCATION '/Users/keeratjohar2305/Downloads/eclipseSbtSpark/SPARK_KAFKA/spark-warehouse/product_catalog/hour=20'""")
spark.sql("""ALTER TABLE product_catalog ADD IF NOT EXISTS
    PARTITION (hour=21) LOCATION '/Users/keeratjohar2305/Downloads/eclipseSbtSpark/SPARK_KAFKA/spark-warehouse/product_catalog/hour=21'""")
spark.sql("""ALTER TABLE product_catalog ADD IF NOT EXISTS
    PARTITION (hour=22) LOCATION '/Users/keeratjohar2305/Downloads/eclipseSbtSpark/SPARK_KAFKA/spark-warehouse/product_catalog/hour=22'""")
spark.sql("""ALTER TABLE product_catalog ADD IF NOT EXISTS
    PARTITION (hour=23) LOCATION '/Users/keeratjohar2305/Downloads/eclipseSbtSpark/SPARK_KAFKA/spark-warehouse/product_catalog/hour=23'""")
spark.sql("""ALTER TABLE product_catalog ADD IF NOT EXISTS
    PARTITION (hour=0) LOCATION '/Users/keeratjohar2305/Downloads/eclipseSbtSpark/SPARK_KAFKA/spark-warehouse/product_catalog/hour=0'""")
    
 println("query execution start")
 spark.sql("select count(1) from product_catalog").show()

 spark.sql("desc table product_catalog").show()

 // Top selling product for a given channel
 
 spark.sql("""select product_id from product_Catalog where  channel = 'Tablet' 
             group by product_id order by count(1) desc limit 5 """).show()
 
  spark.sql("""select product_id from product_Catalog where  channel = 'CAT3' 
             group by product_id order by count(1) desc limit 5 """).show()
 
   spark.sql("""select count(1),  zipcode2, city,state from product_Catalog where  
             group by zipcode2, city,state order by count(1) desc limit 5 """).show()
             
             
  }
}
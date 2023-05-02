import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions.countDistinct
import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType


object nucleusteq extends App {

  val spark = SparkSession.builder()
    .appName("assignment")
    .master("local[*]")
    .getOrCreate()

  val explicitschema = StructType(List(
    StructField("order_item_id",IntegerType),
    StructField("order_item_order_id",IntegerType),
    StructField("order_item_product_id",IntegerType),
    StructField("order_item_quantity",IntegerType),
    StructField("order_item_subtotal",FloatType),
    StructField("order_item_product_price",FloatType)
  ))

  val explicitschema1 = StructType(List(
    StructField("order_id",IntegerType),
    StructField("order_date",StringType,true),
    StructField("order_customer_id",IntegerType),
    StructField("order_status",StringType)
  ))



  val first_file = spark.read
    .format("csv")
    .schema(explicitschema)
    .option("path","C:\\Users\\suraj\\OneDrive\\Desktop\\Suraj\\Big_Data\\dataset\\orders_items.txt")
    .load

  val second_file = spark.read
    .format("csv")
    .option("header", true)
    .schema(explicitschema1)
    .option("path","C:\\Users\\suraj\\OneDrive\\Desktop\\Suraj\\Big_Data\\dataset\\orderstatus.txt")
    .load

  val join_condition=first_file.col("order_item_order_id")===second_file.col("order_id")
  val join_type = "inner"
  val joinhere = first_file.join(second_file,join_condition,join_type)
  /* joinhere.createOrReplaceTempView("joinedtable")
  
  spark.sql("select * from joinedtable").show()*/

  /*joinhere.groupBy(to_date(col("order_date")).alias("order_formatted_date"),col("order_status"))
  .agg(round(sum("order_item_subtotal"), 2).alias("total_amount"),countDistinct("order_id").alias("total_orders"))
  .orderBy(col("order_formatted_date").desc,col("order_status"),col("total_amount"),col("total_orders"))
  .show*/
  joinhere.select(avg(col("order_item_order_id"))).show()

  //scala.io.StdIn.readLine
  spark.stop()
}
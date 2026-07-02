from pyspark.sql import SparkSession, functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType

spark = SparkSession.builder.appName("Customer Totals").getOrCreate()

schema = StructType([
    StructField("customerId",IntegerType(),False),
    StructField("orderId",IntegerType(),False),
    StructField("amount",FloatType(),False),]
)

customerDf = spark.read.csv("./customer-orders.csv",header = False,schema=schema).select("customerId","amount")

#customerDf.show()

customerTotals = customerDf.groupBy("customerId").agg(func.sum("amount")).show()

spark.stop()

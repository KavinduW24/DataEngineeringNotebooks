from pyspark.sql import SparkSession, functions as f


spark = SparkSession.builder.appName("NoOrders").getOrCreate()



customers = spark.read.options(header=True, inferSchema=True).csv("./customers.csv")

orders = spark.read.options(header=True, inferSchema=True).csv("./orders.csv")

customers.join(orders,"customer_id","full").where(f.col("order_id").isNull()).select("customer_id","first_name","last_name").show()
    
    
spark.stop()

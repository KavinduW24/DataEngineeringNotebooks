from pyspark.sql import SparkSession, functions as f
from pyspark.sql.types import StructField, StructType, IntegerType, StringType

spark = SparkSession.builder.appName("Least Popular Hero").getOrCreate()

schema = StructType([
    StructField("id", IntegerType()),
    StructField("name", StringType())
    ])

names = spark.read.schema(schema).option("sep"," ").csv("./MarvelNames.txt")

lines = spark.read.text("./MarvelGraph.txt")

connections = lines.withColumn('id', f.split(f.col("value"), " ")[0])\
    .withColumn("connections", f.size(f.split(f.col("value"), " ")) - 1)

minimun = connections.agg(f.min("connections")).first()[0]

minConnections = connections.filter(f.col("connections") == minimun).select("id","connections")

minConnections.join(names,"id").show()

spark.stop()

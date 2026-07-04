from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType

spark = SparkSession.builder.appName("popular movies").getOrCreate()

schema = StructType([
    StructField("user_id",IntegerType()),
    StructField("item_id",IntegerType()),
    StructField("rating", IntegerType()),
    StructField("timestamp", IntegerType())])

moviesDf = spark.read.option("sep","\t").schema(schema=schema).csv("./ml-100k/u.data")

#moviesDf.show()

topMoviesDf = moviesDf.groupBy("item_id").count().orderBy("count", ascending = False).show(10)

spark.stop()

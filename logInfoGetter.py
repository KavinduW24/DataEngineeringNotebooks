from pyspark.sql import SparkSession, functions as func
from pyspark.sql.functions import udf, udtf
from pyspark.sql.types import StringType
import re



spark = SparkSession.builder.appName("LogGetter").getOrCreate()

textDf = spark.read.text("./access_log.txt")
# textDf.show(5)
@udf(StringType())
def get_ip(line):
    return re.findall(r'(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})', line)[0] if re.findall(r'(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})', line) else None
@udf(StringType())
def get_endpoint(line):
    return re.findall(r'\"(GET|POST|PUT|DELETE|HEAD) (.*?) HTTP', line)[0][0] if re.findall(r'\"(GET|POST|PUT|DELETE|HEAD) (.*?) HTTP', line) else None

@udf(StringType())
def get_status_code(line):
    return re.findall(r'\" (\d{3}) ', line)[0] if re.findall(r'\" (\d{3}) ', line) else None

spark.udf.register("get_ip",get_ip)
spark.udf.register("get_endpoint",get_endpoint)
spark.udf.register("get_status_code",get_status_code)
textDf.createOrReplaceTempView("textDf")


# List IP addresses + sum of their requests
spark.sql("SELECT get_ip(value) AS ip_address, COUNT(*) AS request_count FROM textDf GROUP BY ip_address ORDER BY request_count DESC").show(5)

#ipCounts = textDf.groupBy(func.split(textDf.value, " ")[0].alias("ip address")).count().orderBy("count", ascending =False).show()

# List requested endpoints + count of number of time they have been called
spark.sql("SELECT get_endpoint(value) AS endpoint, COUNT(*) AS request_count FROM textDf GROUP BY endpoint ORDER BY request_count DESC").show(5)
#requestCounts = textDf.groupBy(func.split(textDf.value, " ")[6].alias("endpoint")).count().orderBy("count", ascending =False).show(5)

# List HTTP status codes + count from all requests
spark.sql("SELECT get_status_code(value) AS status_code, COUNT(*) AS request_count FROM textDf GROUP BY status_code ORDER BY request_count DESC").show(5)
#statusCounts = textDf.filter(func.split(textDf.value, " ")[8].alias("code") == '"-"').show()

spark.stop()

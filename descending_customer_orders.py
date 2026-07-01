from pyspark import SparkContext, SparkConf
from prettytable import PrettyTable

conf = SparkConf().setMaster("local[*]").setAppName("CustomerTotals")
sc = SparkContext(conf= conf)

def parse_line(line):
    fields = line.split(",")
    customerId = fields[0]
    order_price = float(fields[2])
    return(customerId,order_price)

lines = sc.textFile("./customer-orders.csv")
parsed_lines = lines.map(parse_line)
customers = parsed_lines.reduceByKey(lambda x, y : x + y)
sorted_customers = customers.sortBy(lambda x: x[1], ascending=False)
    
table = PrettyTable()
table.field_names = ["Customer ID","Total Spending"]

for id,spending in sorted_customers.collect():
    table.add_row([id,spending])

print(table)

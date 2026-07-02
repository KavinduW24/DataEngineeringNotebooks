from pyspark import SparkContext, SparkConf
from prettytable import PrettyTable

conf = SparkConf().setAppName("MaxTemps").setMaster("local[*]")

sc = SparkContext(conf=conf)

def parse_line(line):
    fields = line.split(",")
    station_id = fields[0]
    entry_type = fields[2]
    temp= float(fields[3])
    return (station_id, entry_type, temp)

lines = sc.textFile("weatherData1800s.csv")
parsed_lines = lines.map(parse_line)
max_temps = parsed_lines.filter(lambda x: x[1] == "TMAX")
station_temps = max_temps.map(lambda x: (x[0], x[2]))
max_temps_by_station = station_temps.reduceByKey(max)
results = max_temps_by_station.collect()

table = PrettyTable()
table.field_names = ["Station", "Max Temp"]

for result in results:
    station, temp = result
    table.add_row([station, temp])

print(table)

sc.stop()

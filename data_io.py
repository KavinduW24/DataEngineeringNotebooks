from pyspark import SparkContext

sc = SparkContext("local[*]", "DataIO")

# Load the CSV file
lines = sc.textFile("sales_data.csv")

# Skip header line
header = lines.first()
data = lines.filter(lambda line: line != header)

print(f"Header: {header}")
print(f"Data records: {data.count()}")
print(f"First record: {data.first()}")

def parse_record(line):
    """Parse CSV line into structured data."""
    parts = line.split(",")
    return {
        "product_id": parts[0],
        "name": parts[1],
        "category": parts[2],
        "price": float(parts[3]),
        "quantity": int(parts[4])
    }

# Parse all records
parsed = data.map(parse_record)

# Show parsed data
for record in parsed.take(3):
    print(record)
    
# Calculate revenue for each product
revenue = parsed.map(lambda r: f"{r['product_id']},{r['name']},{r['price'] * r['quantity']:.2f}")

# Save to output directory
# YOUR CODE: Use saveAsTextFile to save revenue data

revenue.saveAsTextFile("revenue_data.csv")

# YOUR CODE: Create sales_data_2.csv with more records
# YOUR CODE: Load all CSV files using wildcard pattern
more_data = [
    "64,4654,12.06",
    "24,8080,18.04",
    "55,3542,22.32"
]
more_data_rdd = sc.parallelize(more_data)
more_data_rdd.saveAsTextFile("sales_data_2.csv")

all_data = sc.textFile("sales_data*.csv")


# YOUR CODE: Use coalesce(1) before saveAsTextFile
# This creates a single output file instead of multiple parts

all_data.coalesce(1).saveAsTextFile("all_sales_data.csv")

sc.close()

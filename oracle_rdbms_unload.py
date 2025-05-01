from pyspark.sql import SparkSession

# Step 1: Create Spark session
spark = SparkSession.builder \
    .appName("OracleJDBCUnload") \
    .config("spark.jars", "/path/to/ojdbc8.jar") \   # <-- Path to Oracle JDBC driver
    .getOrCreate()

# JDBC connection properties
jdbc_url = "jdbc:oracle:thin:@//host:port/service_name"
table_name = "your_table"
user = "your_username"
password = "your_password"
partition_column = "your_surrogate_key_column"

# Step 2: Get dynamic lower and upper bound
bounds_query = f"(SELECT MIN({partition_column}) AS min_id, MAX({partition_column}) AS max_id FROM {table_name}) bounds"

bounds_df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", bounds_query) \
    .option("user", user) \
    .option("password", password) \
    .load()

bounds = bounds_df.collect()[0]
lower_bound = bounds['MIN_ID']
upper_bound = bounds['MAX_ID']

print(f"Detected lowerBound: {lower_bound}, upperBound: {upper_bound}")

# Step 3: Now load the table using partitioning
data_df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", table_name) \
    .option("user", user) \
    .option("password", password) \
    .option("partitionColumn", partition_column) \
    .option("lowerBound", lower_bound) \
    .option("upperBound", upper_bound) \
    .option("numPartitions", 10) \
    .load()

# Step 4: Do something with your data
print(data_df.count())
data_df.show()

# -*- coding: utf-8 -*-
"""remove_redundant_pairs


from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType
from datetime import datetime
from pyspark.sql.functions import sequence, explode, lit, to_date,col
from pyspark.sql.functions import min as min_, max as max_
spark = SparkSession.builder.appName("TimeSeriesGapFill").getOrCreate()


data = [
    ('apple', 'samsung', 2020, 1, 2, 1, 2),
    ('samsung', 'apple', 2020, 1, 2, 1, 2),
    ('apple', 'samsung', 2021, 1, 2, 5, 3),
    ('samsung', 'apple', 2021, 5, 3, 1, 2),
    ('google', None, 2020, 5, 9, None, None),
    ('oneplus', 'nothing', 2020, 5, 9, 6, 3)
]
schema = 'brand1 string , brand2 string , year int , custom1 int, custom2 int , custom3 int , custom4 int'

df = spark.createDataFrame(data = data , schema = schema)

from pyspark.sql.functions import when ,concat
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
df3=df.withColumn("pair_id", when(col("brand1") < col("brand2"), concat("brand1","brand2","year")).otherwise(concat("brand2","brand1","year")))

WindowSpec = Window.partitionBy("pair_id").orderBy("pair_id")
df3=df3.withColumn("row_num",row_number().over(WindowSpec))
df3.show()

df3.filter((col("row_num") == 1) | ((col("custom1")!=col("custom3")) | (col("custom2")!=col("custom4")))).show()

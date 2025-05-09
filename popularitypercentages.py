# -*- coding: utf-8 -*-
"""popularityPercentages

Automatically generated by Colab.

Original file is located at
    https://colab.research.google.com/drive/1g0whpY9czuN8_70EhxtjHAepUte6ZEJL
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col,round

spark = SparkSession.builder.appName('GraphApp').getOrCreate()
data = [
    (1,5),
    (1,3),
    (1,6),
    (2,1),
    (2,6),
    (3,9),
    (4,1),
    (7,2),
    (8,3)
]

schema ="user1 int, user2 int"

df = spark.createDataFrame(data = data , schema = schema)
#df.show()

user1=df.groupBy("user1").count()
user1.withColumnRenamed("user1","user")

user2=df.groupBy("user2").count()
user2.withColumnRenamed("user2","user")

users=user1.union(user2)

user_f=users.groupBy("user1").sum("count").withColumnRenamed("sum(count)","count")
#user_f.show()
user_ff=user_f.withColumn("popularity",round((col("count")/9)*100,3))
user_ff.show()




import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id

spark = SparkSession.builder.getOrCreate()

data = [
    (10,20,11,20),
    (20, 11, 10,99),
    (10, 11, 20,  1),
    (30, 12, 20,99),
    (10, 11, 20, 20),
    (40, 13, 15,  3),
    (30, 8, 11, 99)
]
schema = "A int , B int , C int , D int"
df = spark.createDataFrame(data = data , schema = schema)


df_list=[]

for col in df.columns:
    print("processing column ",col)
    df_col=df.select(col).groupBy(col).count()
    df_col=df_col.withColumnRenamed('count', f"Count_{col}")
    df_list.append(df_col)
    print("processed column ",col)

df_list  =    [df.withColumn('index',monotonically_increasing_id()) for df in df_list]

df_final=df_list[0]
#df_final.show()
for df in df_list[1:]:
  df_final=df_final.join(df,on='index',how='full')
  #df_final=df_final.drop('index')

df_final.drop("index").show()


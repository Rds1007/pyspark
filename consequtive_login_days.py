

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date,last,datediff,lag,col,dense_rank
from pyspark.sql.window import Window
from pyspark.sql.functions import max,min,col

spark = SparkSession.builder.appName("test").getOrCreate()

data = [
(1, '2024-03-01'),
(1, '2024-03-02'),
(1, '2024-03-03'),
(1, '2024-03-04'),
(1, '2024-03-06'),
(1, '2024-03-10'),
(1, '2024-03-11'),
(1, '2024-03-12'),
(1, '2024-03-13'),
(1, '2024-03-14'),
(1, '2024-03-20'),
(1, '2024-03-25'),
(1, '2024-03-26'),
(1, '2024-03-27'),
(1, '2024-03-28'),
(1, '2024-03-29'),
(1, '2024-03-30'),
(2, '2024-03-01'),
(2, '2024-03-02'),
(2, '2024-03-03'),
(2, '2024-03-04'),
(3, '2024-03-01'),
(3, '2024-03-02'),
(3, '2024-03-03'),
(3, '2024-03-04'),
(3, '2024-03-04'),
(3, '2024-03-04'),
(3, '2024-03-05'),
(4, '2024-03-01'),
(4, '2024-03-02'),
(4, '2024-03-03'),
(4, '2024-03-04'),
(4, '2024-03-04')
]

schema = "user_id int , login_date string"

df = spark.createDataFrame(data = data , schema = schema)

df2=df.withColumn("login_date", to_date("login_date", "yyyy-MM-dd"))
df3=df2.drop_duplicates().orderBy("user_id","login_date")

windowSpec = Window.partitionBy("user_id").orderBy("login_date")

df4=df3.withColumn("drank", dense_rank().over(windowSpec))
df5=df4.withColumn("group_",col("login_date")-col("drank"))
df6=df5.groupBy("user_id","group_").agg(
                                      min(col("login_date")).alias("start_date"),
                                      max(col("login_date")).alias("end_date")
)


df7=df6.withColumn("consequtive_days",datediff(col("end_date"),col("start_date"))+1).filter(col("consequtive_days")>=5).select('user_id',"start_date","end_date",col("consequtive_days").alias("duration"))
df7.show()

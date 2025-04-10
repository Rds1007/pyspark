![image](https://github.com/user-attachments/assets/187d5512-5e8c-4b44-be13-201d3613224e)creating the dataframe 
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

display(df)


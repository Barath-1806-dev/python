from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("practise").getOrCreate()





from pyspark.sql.functions import *


data = [
    (1001, 1, 10, 1000),
    (1002, 1, 10, 2000),
    (1003, 1, 10, 3000),
    (1004, 2, 10, 4000),
    (1005, 2, 10, 2000),
    (1006, 3, 5, 800),
    (1007, 3, 5, 600),
    (1008, 3, 5, 500),
    (1009, 4, 3, 400),
    (1010, 4, 2, 200),
    (1010, 6, 2, 200),
    (1001, 1, 5, 500),
    (1001, 1, 5, 500),
    (1001, 1, 5, 500),
    (1003, 1, 10, 3000)
]


columns = ["ProductId", "Category", "Units", "SalesValue"]

df = spark.createDataFrame(data, columns)
df.show()

data2 = [
    (1, "Cat-1"),
    (2, "Cat-2"),
    (3, "Cat-3"),
    (4, "Cat-4"),
    (5, "Cat-5")
]

columns2 = ["Category1","Description"]

df2 = spark.createDataFrame(data2, columns2)
df2.show()

Aggdf = df.groupBy("Category").agg(sum("SalesValue").alias("TotalSalesValue"))
Aggdf.show()

joindf = df2.join(Aggdf, df2.Category1 == Aggdf.Category, "left")
joindf.fillna(0).drop("Category1").show()


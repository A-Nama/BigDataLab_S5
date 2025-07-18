from pyspark.sql import SparkSession

spark = SparkSession.builder\
    .appName("Pyspark test")\
        .getOrCreate()
        
data = [("Alice", 25), ("Bob", 30)]
df = spark.createDataFrame(data, ["Name", "Age"])

df.show()

spark.stop()
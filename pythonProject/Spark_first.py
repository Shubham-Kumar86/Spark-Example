import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

os.environ["PYSPARK_PYTHON"] = "PYTHON"

spark = SparkSession.builder.appName("PySparkExample").getOrCreate()

data = [("Alice", 25), ("Bob", 30), ("Charlie", 35), ("David", 40)]
columns = ["Name", "Age"]

df = spark.createDataFrame(data, columns)

df.show()

df = df.withColumn("Age", col("Age") + 5)

df.show()

df_filtered = df.filter(col("Age") >= 35)

df_filtered.show()

spark.stop()

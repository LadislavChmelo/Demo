import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import trim

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder.getOrCreate()
# sample1_df = spark.read.csv(r"C:\Users\lchme\PycharmProjects\sample_data\100 Sales Records.csv", inferSchema=True, header=True)
sample2_df = spark.read.text(r"C:\Users\lchme\PycharmProjects\sample_data\names.txt")

# sample1_df.show()
sample3_df = sample2_df.select(trim("value"))
print(sample3_df.count())




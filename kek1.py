import sys

from pyspark.shell import spark
from pyspark.sql.window import Window
import pyspark.sql.functions as func

d = [{'session': 1, 'ts': 1}, {'session': 1, 'ts': 2, 'id': 109}, {'session': 1, 'ts': 3}, {'session': 1, 'ts': 4, 'id': 110}, {'session': 1, 'ts': 5},  {'session': 1, 'ts': 6}]
df = spark.createDataFrame(d)

df.withColumn("id", func.last('id', True).over(Window.partitionBy('session').orderBy('ts').rowsBetween(-sys.maxsize, 0))).show()


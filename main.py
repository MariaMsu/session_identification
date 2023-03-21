from pyspark.shell import spark
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.window import Window

import config

# Load the data into a Spark DataFrame, assuming the data is in a CSV file with headers:
df = spark.read.format("csv").option("header", "true").load(config.input_path)

# Convert timestamp column to unix time
df = df.withColumn("timestamp_long", F.unix_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss").cast("long"))

# Define a window specification based on the user_id and product_code columns, ordered by the timestamp column:
window_spec = Window.partitionBy("user_id", "product_code").orderBy("timestamp")

# Compute the time difference between consecutive events within each window and create a new column
df = df.withColumn("prev_timestamp", F.lag("timestamp_long", 1).over(window_spec))
df = df.withColumn("time_diff",
                   F.when(df.prev_timestamp.isNull(), None).otherwise(df.timestamp_long - df.prev_timestamp))
df = df.withColumn("new_session", ((df.time_diff > config.session_time_threshold) | (df.time_diff.isNull())))

# fill session_id column for lines, representing new session
df = df.withColumn("session_id", F.when(df.new_session == 0, None).otherwise(
    F.concat_ws("#", "user_id", "product_code", "timestamp")
))

# create a window partitioned by date in ascending order
window1 = Window.partitionBy('user_id').orderBy("timestamp_long") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

# fill the empty event strings with the value from the closest previous row containing an event name
df = df.withColumn('session_id', F.when(df['session_id'].isNull(), F.last('session_id', True)
                                        .over(window1)).otherwise(df['session_id']))

# Drop the intermediate columns
# df = df.drop("prev_timestamp", "time_diff", "new_session")

# Get boolean columns' names
bool_columns = [col[0] for col in df.dtypes if col[1] == 'boolean']
# Cast boolean to Integers
for col in bool_columns:
    dft = df.withColumn(col, F.col(col).cast(T.IntegerType()))
result_df_path = config.output_path
dft.toPandas().to_csv(result_df_path)

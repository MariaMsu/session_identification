from pyspark.shell import spark

# Load the data into a Spark DataFrame, assuming the data is in a CSV file with headers:
df = spark.read.format("csv").option("header", "true").load("/home/omar/Desktop/spark/test_data.csv")

# Convert the timestamp column to a TimestampType and create a new column with the date component:
from pyspark.sql.functions import unix_timestamp, from_unixtime, date_format

df = df.withColumn("timestamp_long", unix_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss").cast("long"))

# Define a window specification based on the user_id, product_code, and date columns, ordered by the timestamp column:
from pyspark.sql.window import Window
from pyspark.sql.functions import lag, when

window_spec = Window.partitionBy("user_id", "product_code").orderBy("timestamp")

# Compute the time difference between consecutive events within each window and create a new column
# with a boolean indicating if the time difference is greater than a threshold value (e.g., 30 minutes):
df = df.withColumn("prev_timestamp", lag("timestamp_long", 1).over(window_spec))
df = df.withColumn("time_diff", when(df.prev_timestamp.isNull(), 0).otherwise((df.timestamp_long - df.prev_timestamp) / 60))
df = df.withColumn("new_session", df.time_diff > 30)  # TODO this does not work, fix it
df = df.withColumn("new_session", df.time_diff == 0)

# Compute a running sum of the new_session column within each window to create a unique session identifier for each event:
from pyspark.sql.functions import sum, concat_ws

import pyspark.sql.functions as F
import pyspark.sql.types as T
df = df.withColumn("session_id", F.when(df.new_session == 0, None).otherwise(
    concat_ws("#", "user_id", "product_code", "timestamp")
))

# create a window partitioned by date in ascending order
window1 = Window.partitionBy('user_id').orderBy("timestamp_long").rowsBetween(Window.unboundedPreceding, Window.currentRow)

# fill the empty event strings with the value from the closest previous row containing an event name
df = df.withColumn('session_id', when(df['session_id'].isNull(), F.last('session_id', True).over(window1)).otherwise(df['session_id']))


# df = df.withColumn("session_id", F.first('timestamp_long').filter('new_session == 1').over(window_spec))
# df = df.withColumn("session_id", sum("new_session").over(window_spec))
# df = df.withColumn("session_id", concat_ws("#", "user_id", "product_code", date_format("timestamp_long", "yyyy-MM-dd"), "session_id"))

# Drop the intermediate columns
# df = df.drop("prev_timestamp", "time_diff", "new_session")


# Get boolean columns' names
bool_columns = [col[0] for col in df.dtypes if col[1] == 'boolean']
# Cast boolean to Integers
for col in bool_columns:
    dft = df.withColumn(col, F.col(col).cast(T.IntegerType()))
result_df_path = "/home/omar/Desktop/spark/test_out_data.csv"
dft.toPandas().to_csv(result_df_path)


# The final session_id column will have the desired format of user_id#product_code#timestamp#session_id.
# Note that you can adjust the threshold value and the format of the timestamp component based on your requirements.


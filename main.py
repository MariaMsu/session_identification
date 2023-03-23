from pyspark.shell import spark
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.window import Window

import config

"""
compute user_session_id based on events timestamps, user_ids and product_ids
"""


def compute_session_id(df, session_time_threshold, debug=False):
    # Convert timestamp column to unix time
    df = df.withColumn("timestamp_long", F.unix_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss").cast("long"))

    # Define a window specification based on the user_id and product_code columns, ordered by the timestamp_long column:
    window_spec = Window.partitionBy("user_id", "product_code").orderBy("timestamp_long")

    # Compute the time difference between consecutive events within each window and create a new column
    df = df.withColumn("prev_timestamp", F.lag("timestamp_long", 1).over(window_spec))
    df = df.withColumn("time_diff",
                       F.when(df.prev_timestamp.isNull(), None).otherwise(df.timestamp_long - df.prev_timestamp))

    # detect start of the new session
    df = df.withColumn("new_session", ((df.time_diff > session_time_threshold) | (df.time_diff.isNull())))

    # fill session_id column for lines, representing new session
    df = df.withColumn("session_id", F.when(df.new_session == 0, None).otherwise(
        F.concat_ws("#", "user_id", "product_code", "timestamp")
    ))

    # fill the empty event strings with the value from the closest previous row containing an event name
    df = df.withColumn('session_id', F.when(df['session_id'].isNull(), F.last('session_id', True)
                                            .over(window_spec)).otherwise(df['session_id']))

    if debug:
        # Drop the intermediate columns
        df = df.drop("prev_timestamp", "time_diff", "new_session", "timestamp_long")

    return df


def compute_session_id2(df, debug=False):
    # Convert timestamp column to unix time
    df = df.withColumn("timestamp_long", F.unix_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss").cast("long"))

    # Define a window specification based on the user_id and product_code columns, ordered by the timestamp_long column:
    window_spec_incr = Window.partitionBy("user_id", "product_code").orderBy("timestamp_long")
    # fill session_id column for lines, representing new session
    df = df.withColumn("session_id", F.when(df.event_id != "ide.start", None).otherwise(
        F.concat_ws("#", "user_id", "product_code", "timestamp")
    ))
    # fill the empty event strings with the value from the closest previous row containing an event name
    df = df.withColumn('session_id', F.when(df['session_id'].isNull(), F.last('session_id', True)
                                                 .over(window_spec_incr)).otherwise(df['session_id']))

    w1 = Window.partitionBy("session_id").orderBy("timestamp_long")
    df = df.withColumn('after_close', F.when(F.lag(df.event_id, 1).over(w1) == "ide.close", 1))
    # fill the empty event strings with the value from the closest previous row containing an event name
    df = df.withColumn('after_close', F.when(df['after_close'].isNull(), F.last('after_close', True)
                                             .over(w1)).otherwise(df['after_close']))

    df = df.withColumn('session_id', F.when(F.col('after_close').isNull(), F.col("session_id")))

    if debug:
        # Drop the intermediate columns
        df = df.drop("after_close", "timestamp_long")
    return df


def write_table(df, output_path):
    # write new table to the output_path
    # Get boolean columns' names
    bool_columns = [col[0] for col in df.dtypes if col[1] == 'boolean']
    # Cast boolean to Integers
    for col in bool_columns:
        df = df.withColumn(col, F.col(col).cast(T.IntegerType()))
    print(f'    Write data to {output_path}')
    df.toPandas().to_csv(output_path, index=False)


if __name__ == "__main__":
    # Load the data into a Spark DataFrame, assuming the data is in a CSV file with headers:
    print(f'    Read date from {config.input_path}')
    df_initial = spark.read.format("csv").option("header", "true").load(config.input_path)
    df_with_ids1 = compute_session_id(
        df=df_initial,
        session_time_threshold=config.session_time_threshold,
        debug=False)
    write_table(df_with_ids1, output_path=config.output_path1)

    df_with_ids2 = compute_session_id2(
        df=df_initial,
        debug=False)
    write_table(df_with_ids2, output_path=config.output_path2)

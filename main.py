from pyspark.shell import spark
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.window import Window

import config


def compute_session_id(df, session_time_threshold, debug=False):
    # Convert timestamp column to unix time
    df = df.withColumn("timestamp_long", F.unix_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss").cast("long"))

    # Define a window specification based on the user_id and product_code columns, ordered by the timestamp_long column:
    window_spec = Window.partitionBy("user_id", "product_code").orderBy("timestamp_long")

    # Compute the time difference between consecutive events within each window and create a new column
    df = df.withColumn("prev_timestamp", F.lag("timestamp_long", 1).over(window_spec))
    df = df.withColumn("time_diff",
                       F.when(df.prev_timestamp.isNull(), None).otherwise(df.timestamp_long - df.prev_timestamp))
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
        df = df.drop("prev_timestamp", "time_diff", "new_session")

    return df


if __name__ == "__main__":
    # Load the data into a Spark DataFrame, assuming the data is in a CSV file with headers:
    df_initial = spark.read.format("csv").option("header", "true").load(config.input_path)
    df_with_ids = compute_session_id(
        df=df_initial,
        session_time_threshold=config.session_time_threshold,
        debug=False)

    # Get boolean columns' names
    bool_columns = [col[0] for col in df_with_ids.dtypes if col[1] == 'boolean']
    # Cast boolean to Integers
    for col in bool_columns:
        dft = df_with_ids.withColumn(col, F.col(col).cast(T.IntegerType()))
    dft.toPandas().to_csv(config.output_path)

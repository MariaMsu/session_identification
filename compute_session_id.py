from pyspark.shell import spark
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.window import Window

START_ACTION = 'ide.start'
CLOSE_ACTION = 'ide.close'
TIMESTAMP_FORMAT = 'yyyy-MM-dd HH:mm:ss'


def compute_session_id_time_bound(df, session_time_threshold, debug=False):
    """
    Compute user_session_id based on timestamps, user_ids and product_ids.

    This function considers a user session is a set of actions performed by a user or an IDE
    with a short time interval 'session_time_threshold' between these actions.
    With this policy, all the rows get a user session id.
    """

    # Convert timestamp column to unix time
    df = df.withColumn('timestamp_long', F.unix_timestamp('timestamp', TIMESTAMP_FORMAT).cast('long'))

    # Define a window specification based on the user_id and product_code columns, ordered by the timestamp_long column:
    w_user_product = Window.partitionBy('user_id', 'product_code').orderBy('timestamp_long')

    # Compute the time difference between consecutive events within each window and create a new column
    df = df.withColumn('prev_timestamp', F.lag('timestamp_long', 1).over(w_user_product))
    df = df.withColumn('time_diff',
                       F.when(df.prev_timestamp.isNull(), None).otherwise(df.timestamp_long - df.prev_timestamp))

    # detect start of the new session & fill the session_id column for the lines, representing start of the session
    df = df.withColumn('session_id', F.when(((df.time_diff > session_time_threshold) | (df.time_diff.isNull())),
                                            F.concat_ws('#', 'user_id', 'product_code', 'timestamp')))

    # fill the empty session_id strings with the value from the closest previous row containing a session_id
    df = df.withColumn('session_id', F.when(df['session_id'].isNull(), F.last('session_id', True)
                                            .over(w_user_product)).otherwise(df['session_id']))

    if not debug:
        # Drop the intermediate columns
        df = df.drop('prev_timestamp', 'time_diff', 'timestamp_long')

    return df


def compute_session_id_start_close(df, debug=False):
    """
    Compute user_session_id based on event_ids, timestamps, user_ids and product_ids.

    This function considers a user session is a set of all events for a distinct user that happened
    between the events 'START_ACTION' and 'CLOSE_ACTION'.
    If an ide was opened but was not yet closed, this set of actions is also considered as a session.
    With this policy, some rows do not belong to any user session.
    """

    # Convert timestamp column to unix time
    df = df.withColumn('timestamp_long', F.unix_timestamp('timestamp', TIMESTAMP_FORMAT).cast('long'))

    # Define a window specification based on the user_id and product_code columns, ordered by the timestamp_long column:
    w_user_product = Window.partitionBy('user_id', 'product_code').orderBy('timestamp_long')

    # fill session_id column for lines, representing start of the session
    df = df.withColumn('session_id', F.when(df.event_id == START_ACTION,
                                            F.concat_ws('#', 'user_id', 'product_code', 'timestamp')))

    # fill the empty session_id strings with the value from the closest previous row containing a session_id
    df = df.withColumn('session_id', F.when(df['session_id'].isNull(), F.last('session_id', True)
                                            .over(w_user_product)).otherwise(df['session_id']))

    # define rows which represent the actions happened after IDE was closed
    w_session = Window.partitionBy('session_id').orderBy('timestamp_long')
    df = df.withColumn('after_close', F.when(F.lag(df.event_id, 1).over(w_session) == CLOSE_ACTION, 1))
    df = df.withColumn('after_close', F.when(df['after_close'].isNull(), F.last('after_close', True)
                                             .over(w_session)).otherwise(df['after_close']))

    # remove session_id from the rows representing actions after IDE was closed
    df = df.withColumn('session_id', F.when(F.col('after_close').isNull(), F.col('session_id')))

    if not debug:
        # Drop the intermediate columns
        df = df.drop('after_close', 'timestamp_long')
    return df


def write_table(df, output_path):
    """
    write spark dataframe 'df' to 'output_path'
    """
    # write new table to the output_path
    # Get boolean columns' names
    bool_columns = [col[0] for col in df.dtypes if col[1] == 'boolean']
    # Cast boolean to Integers
    for col in bool_columns:
        df = df.withColumn(col, F.col(col).cast(T.IntegerType()))
    print(f'    Write data to {output_path}')
    df.toPandas().to_csv(output_path, index=False)


if __name__ == '__main__':
    # default paths
    input_path = './data/test_in.csv'
    output_path1 = './data/test_out_tb.csv'
    output_path2 = './data/test_out_sc.csv'
    # default time threshold
    session_time_threshold = 30 * 60  # in seconds

    # Load the data into a Spark DataFrame, assuming the data is in a CSV file with headers:
    print(f'    Read date from {input_path}')
    df_initial = spark.read.format('csv').option('header', 'true').load(input_path)

    df_with_ids1 = compute_session_id_time_bound(
        df=df_initial,
        session_time_threshold=session_time_threshold,
        debug=False)
    write_table(df_with_ids1, output_path=output_path1)

    df_with_ids2 = compute_session_id_start_close(
        df=df_initial,
        debug=False)
    write_table(df_with_ids2, output_path=output_path2)

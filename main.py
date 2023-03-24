import argparse

from pyspark.shell import spark

import compute_session_id as csi


def check_positive(value):
    ivalue = int(value)
    if ivalue <= 0:
        raise argparse.ArgumentTypeError('%s is an invalid positive int value' % value)
    return ivalue


def main():
    """
    Read command line arguments and call the corresponding table handler
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input', type=str, help='input file path')
    parser.add_argument('-o', '--output', type=str, help='output file path')
    parser.add_argument('-p', '--policy', choices=['tb', 'sc'],
                        help='user session definition policy: "tb" (time bounded) or "sc" (start & close)')
    parser.add_argument('-t', '--threshold', type=check_positive,
                        help='time threshold for the tb policy in seconds', default=30 * 60)
    args = parser.parse_args()

    # Load the data into a Spark DataFrame, assuming the data is in a CSV file with headers:
    print(f'    Read date from {args.input}')
    df = spark.read.format('csv').option('header', 'true').load(args.input)

    if args.policy == 'tb':
        df = csi.compute_session_id_time_bound(df=df, session_time_threshold=args.threshold)
        csi.write_table(df=df, output_path=args.output)
    elif args.policy == 'sc':
        df = csi.compute_session_id_start_close(df=df)
        csi.write_table(df=df, output_path=args.output)


if __name__ == '__main__':
    main()

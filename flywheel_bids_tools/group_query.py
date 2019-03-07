import pandas as pd
import argparse
from tqdm import tqdm


def read_flywheel_csv(fpath, required_cols=['acquisition.label']):
    '''
    Read in a CSV and also ensure it's one of ours

    Input:
        fpath: path to the file
        required_cols: list of columns to ensure csv is a flywheel query
    Output:
        df: a pandas dataframe
    '''

    df = pd.read_csv(fpath)

    if not all(elem in df.columns.tolist() for elem in required_cols):
        raise Exception(("It doesn't look like this csv is correctly formatted",
        " for this flywheel editing process!"))

    return(df)


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-input", "--input-file",
        dest='infile',
        help="Flywheel query file",
        required=True
    )
    parser.add_argument(
        "-grp-out", "--grouped-output",
        help="The path and name of a grouped version of the output CSV of the query",
        dest="group_output",
        required=True
    )
    parser.add_argument(
        "-grp", "--groupings",
        nargs='+',
        dest='group',
        help="Columns to group unique rows by",
        required=True
    )
    args = parser.parse_args()

    query_result = read_flywheel_csv(args.infile)
    grouped = query_result.copy()
    grouped = grouped.drop_duplicates(args.group)
    reorder = [x for x in args.group] + \
        [x for x in grouped.columns if x not in args.group]
    grouped = grouped[reorder]
    grouped.to_csv(args.group_output, index=True)
    print("Done")


if __name__ == '__main__':
    main()

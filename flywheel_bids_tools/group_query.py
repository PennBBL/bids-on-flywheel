import pandas as pd
import argparse
from tqdm import tqdm
from .query_bids import unlist_item


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
        "-output", "--grouped-output",
        help="The path and name of a grouped version of the output CSV of the query",
        dest="group_output",
        required=True
    )
    parser.add_argument(
        "-groups", "--groupings",
        nargs='+',
        dest='group',
        help="Columns to group unique rows by",
        required=True
    )
    args = parser.parse_args()

    # read in the file
    query_result = read_flywheel_csv(args.infile)

    # add a group index, group
    query_result['group_id'] = (query_result
        # groupby and keep the columns as columns
        .groupby(args.group, as_index=False)
        # index the groups
        .ngroup()
        .add(1))
    query_result = (query_result
        # groupby and sample 1 exemplar
        .groupby(args.group, as_index=False)
        .nth(1)
        .reset_index(drop=True))
    # add index for group indeces
    query_result['groups'] = unlist_item(args.group)
    query_result.to_csv(args.group_output, index=False)
    print("Done")


if __name__ == '__main__':
    main()

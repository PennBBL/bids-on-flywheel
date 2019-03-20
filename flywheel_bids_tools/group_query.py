import pandas as pd
import argparse
from flywheel_bids_tools.utils import unlist_item


def read_flywheel_csv(fpath, required_cols=['acquisition.id']):
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
    #drop_downs = ['classification_Measurement', 'classification_Intent',
        #'classification_Features']
    #df.loc[:, drop_downs] = df.loc[:, drop_downs].applymap(unlist_item)
    df = df.sort_values(by=["acquisition.id", "acquisition.label"])
    return(df)


def main():

    parser = argparse.ArgumentParser(description=("Use this tool to group a Flywheel query file by a column with common values."))
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

    # # add a group index, group
    group_id = query_result.duplicated(args.groups)
    group_id = ~group_id
    group_id2 = group_id.cumsum()
    query_result['group_id'] = group_id2
    # query_result = (query_result
    #     # groupby and sample 1 exemplar
    #     .groupby(args.group, as_index=False)
    #     .nth(1)
    #     .reset_index(drop=True))
    # # add index for group indeces
    # print(query_result)
    query_result = query_result.drop_duplicates(args.group)
    query_result['groups'] = unlist_item(args.group)
    query_result = query_result.sort_values(by=["acquisition.id", "acquisition.label"])
    query_result.to_csv(args.group_output, index=False)
    print("Done")


if __name__ == '__main__':
    main()

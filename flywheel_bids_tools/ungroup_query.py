import pandas as pd
import argparse
from flywheel_bids_tools.upload_bids import get_unequal_cells
from flywheel_bids_tools.utils import relist_item


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
    df = df.sort_values(by=["acquisition.id", "acquisition.label"])
    return(df)


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-grouped", "--grouped-input",
        dest='grouped',
        help="The original grouped Flywheel query file",
        required=True
    )
    parser.add_argument(
        "-mod", "--modified-input",
        help="The modified grouped query file",
        dest="modified",
        required=True
    )
    parser.add_argument(
        "-orig", "--original-input",
        dest='original',
        help="The original ungrouped Flywheel query file",
        required=True
    )
    parser.add_argument(
        "-output", "--output-file",
        help="Path and name of the desired output file with grouped changes mapped to the full dataset",
        dest="output",
        required=True
    )

    args = parser.parse_args()

    # grouped edited df
    df_grouped_modified = read_flywheel_csv(args.modified)
    groups = relist_item(df_grouped_modified['groups'][0])

    # grouped unedited
    df_grouped = read_flywheel_csv(args.grouped)

    # original df
    df_original = read_flywheel_csv(args.original)

    # add groupings
    # add a group index
    df_original['group_id'] = float('nan')
    # group the file and store separate groups
    grouped_df = df_original.groupby(groups).groups

    # loop over groups and assign the index
    id = 1
    for name, df in grouped_df.items():

        index = df.to_list()
        df_original.loc[index, 'group_id'] = id
        id += 1
    # index the differences
    diff = get_unequal_cells(df_grouped, df_grouped_modified, provenance=True)

    changes = []

    for x in diff:

        key = df_grouped_modified.loc[x[0], 'group_id']
        val = (df_grouped_modified.columns[x[1]], df_grouped_modified.iloc[x[0], x[1]])
        changes.append((key, val))

    # loop through the differences and map them to the full dataset
    print("Applying the changes to the full dataset...")

    for change in changes:
        df_original.loc[df_original['group_id'] == change[0], change[1][0]] = change[1][1]

    df_original.drop(columns='group_id', inplace=True)
    df_original = df_original.sort_values(by=["acquisition.id", "acquisition.label"])
    df_original.to_csv(args.output, index=False)
    print("Done")


if __name__ == '__main__':
    main()

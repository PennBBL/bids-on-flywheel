import pandas as pd
import argparse
from tqdm import tqdm
from .upload_bids import get_unequal_cells, relist_item


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
    df_original['group_id'] = (df_original
        # groupby and keep the columns as columns
        .groupby(groups, as_index=False)
        # index the groups
        .ngroup()
        .add(1))

    # index the differences
    diff = get_unequal_cells(df_grouped_modified, df_grouped)

    changes = {}

    for x in diff:

        key = df_grouped_modified.loc[x[0], 'group_id']
        val = (df_grouped_modified.columns[x[1]], df_grouped_modified.iloc[x[0], x[1]])
        changes.update({key: val})

    # loop through the differences and map them to the full dataset
    print("Applying the changes to the full dataset...")

    for group, change in changes.items():
        df_original.loc[df_original['group_id'] == group, change[0]] = change[1]

    df_original.drop(columns='group_id', inplace=True)
    df_original.to_csv(args.output, index=False)
    print("Done")


if __name__ == '__main__':
    main()

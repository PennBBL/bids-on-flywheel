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
        help="Grouped Flywheel query file",
        required=True
    )
    parser.add_argument(
        "-output", "--output-file",
        help="The path and name of the ungrouped output CSV",
        dest="output",
        required=True
    )
    parser.add_argument(
        "-original", "--original-file",
        help="Path to the original flywheel query CSV",
        dest="original",
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

    # original df
    df_original = read_flywheel_csv(args.original)
    # edited df
    df_grouped_modified = read_flywheel_csv(args.infile)
    df_modified = df_original.copy()
    df_modified.update(df_grouped_modified)
    df_modified.to_csv(args.output, index=True)
    print("Done")


if __name__ == '__main__':
    main()

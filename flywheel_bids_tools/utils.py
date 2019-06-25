import pandas as pd
import numpy as np
from datetime import datetime
import os


def unlist_item(ls):
    '''
    Convert a list item to a comma-separated string
    '''
    if isinstance(ls, list):
        ls.sort()
        return(', '.join(x for x in ls))
    else:
        return float('nan')


def relist_item(string):
    '''
    Convert a comma-separated string into a list
    '''
    if type(string) is str:
        return([s.strip() for s in string.split(',')])
    else:
        return string


def read_flywheel_csv(fpath):
    '''
    Read in a CSV and also ensure it's one of ours

    Input:
        fpath: path to the file
        required_cols: list of columns to ensure csv is a flywheel query
    Output:
        df: a pandas dataframe
    '''

    df = pd.read_csv(fpath)
    required_cols=['acquisition.id', 'acquisition.label', 'name', 'session.id', 'subject.id', 'session.label', 'subject.label']
    try:
        for col in required_cols:
            df[col] = df[col].astype(str)
    except KeyError as e:
        print("Column missing:{}".format(e))
        SystemExit(0)

    df = df.reindex(sorted(df.columns), axis=1)
    df = df.sort_values(by=['acquisition.id', 'acquisition.label', 'name'], ascending=False).reset_index(drop=True)

    return(df)


def get_unequal_cells(df1, df2, provenance=True):
    '''
    Compare two dataframes and return indeces where the values are not equal

    Input:
    -------
        df1: original pandas dataframe
        df2: modified pandas dataframe
        provenance: boolean; write out a log of proposed changes

    Output:
    --------
        indices: list of lists--the row-column pairs of unequal cells
    '''

    if df1.shape != df2.shape:
        raise Exception("These dataframes don't have the same number of rows and columns")
    else:
        comparison_array = df1.fillna(0).values == df2.fillna(0).values
        indices = np.where(comparison_array == False)
        indices = np.dstack(indices)[0].tolist()

        if provenance:

            cols = ["original", "modified", "row", "column"]
            lst = []

            for pair in indices:

                original = df1.iloc[pair[0], pair[1]]
                modified = df2.iloc[pair[0], pair[1]]
                column = df1.columns[pair[1]]
                row = df1.index[pair[0]]

                lst.append([original, modified, row, column])

            provenance_df = pd.DataFrame(lst, columns=cols)
            currentDT = datetime.now()
            fname = "provenance_{}.csv".format(currentDT.strftime("%Y-%m-%d_%H:%M:%S"))
            fname_exists = os.path.isfile(fname)
            if fname_exists:
                i = 1
                while fname_exists:
                    i += 1
                    fname = "provenance_{}_{}.csv".format(currentDT.strftime("%Y-%m-%d_%H:%M:%S"), i)
                    fname_exists = os.path.isfile(fname)


            provenance_df.to_csv(fname, index=False, na_rep="NA")

        return(indices)

def is_nan(x):
    return (x is np.nan or x != x)


def is_list_column(col):
    return("[" in col.to_string() and "{" not in col.to_string())

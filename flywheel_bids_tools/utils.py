import pandas as pd


def unlist_item(ls):
    '''Convert a list item to a comma-separated string
    '''
    if type(ls) is list:
        ls.sort()
        return(', '.join(x for x in ls))
    else:
        return float('nan')


def relist_item(string):
    if type(string) is str:
        return([s.strip() for s in string.split(',')])
    else:
        return string


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
    df = df.sort_values(by="acquisition.id")
    return(df)

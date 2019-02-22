import sys
import pandas as pd
import numpy as np
import re


ERROR_MESSAGES = []


def read_flywheel_csv(fpath, required_cols=['acquisition.label',
    'valid', 'acquisition.id', 'project.label', 'session.label',
    'subject.label', 'Filename', 'Folder', 'IntendedFor', 'Mod',
    'Modality', 'Path', 'Rec', 'Run', 'Task', 'error_message',
    'ignore', 'template']):
    '''
    Read in a CSV and also ensure it's one of ours

    Input:
        fpath: path to the file
        required_cols: list of columns to ensure csv is a flywheel query
    Output:
        df: a pandas dataframe
    '''

    df = pd.read_csv(fpath)

    if set(required_cols) != set(df.columns):
        raise Exception(("It doesn't look like this csv is correctly formatted"
        " for this flywheel editing process!"))

    return(df)


def change_checker(user_input, column):
    '''
    When a user has changed a value in a column, check that the input they
    supplied is in the allowed list of acceptable changes. Note this function
    adjusts cases.

    Input:
        user_input: what the user tried to change
        column: the column they tried to change
    Output:
        boolean: True or False whether the change is acceptable on Flywheel
    '''

    drop_down_bool = {
        'ignore': ['true', 'false'],
        'valid': ['true', 'false']
        }

    drop_down_single_fields = {
        'modality': ['', 'mr', 'ct', 'pet', 'us', 'eeg', 'ieeg', 'x-ray',
            'ecg', 'meg', 'nirs']
        }

    string_fields = ['acquisition.label', 'project.label', 'error_message'
        'subject.label', 'filename', 'folder', 'template',
        'intendedfor', 'mod', 'path', 'rec', 'run', 'task']

    # try boolean drop down option
    if column.lower() in drop_down_bool.keys():
        if str(user_input).lower() == 'true' or str(user_input).lower() == 'false':
            return True
        else:
            ERROR_MESSAGES.append("This field accepts booleans, these can only be written as \"True\" or \"False\"!")
            return False

    # try drop down string option
    elif column.lower() in drop_down_single_fields.keys():
        for field, options in drop_down_single_fields.iteritems():
            if str(user_input).lower() in options:
                return True
            else:
                continue
        ERROR_MESSAGES.append("This field must match one of the available options in the drop-down menu on the website!")
        return False

    # try generic string
    elif column.lower() in string_fields:
        if type(user_input) is str:
            return True
        else:
            ERROR_MESSAGES.append("This field only accepts strings!")
            return False

    # try the session.label separately
    elif column == 'session.label':
        expr = re.compile(
            '^sub-(?P<subject_id>[a-zA-Z0-9]+)(_ses-(?P<session_id>[a-zA-Z0-9]+))?'
            '(_task-(?P<task_id>[a-zA-Z0-9]+))?(_acq-(?P<acq_id>[a-zA-Z0-9]+))?'
            '(_rec-(?P<rec_id>[a-zA-Z0-9]+))?(_run-(?P<run_id>[a-zA-Z0-9]+))?')
        if bool(expr.match(str(user_input).lower())):
            return True
        else:
            ERROR_MESSAGES.append("This field MUST be a BIDS compliant name!")
            return False

    # can't edit acquisition ID tho!
    elif column == 'acquisition.id':
        ERROR_MESSAGES.append("You cannot edit the acquisition ID!")
        return False
    else:
        raise Exception("Column {0} not recognised!".format(column))


def get_unequal_cells(df1, df2, provenance=True):
    '''
    Compare two dataframes and return indeces where the values are not equal

    Input:
        df1: original pandas dataframe
        df2: modified pandas dataframe
        provenance: boolean; write out a log of proposed changes
    Output:
        indices: list of lists--the row-column pairs of unequal cells
    '''

    if df1.shape != df2.shape:
        raise Exception("These dataframes don't have the same number of rows and columns")
    else:
        comparison_array = df1.fillna(0).values == df2.fillna(0).values
        indices = np.where(comparison_array == False)
        indices = np.dstack(indices)[0].tolist()

        if provenance:

            cols = ["original", "modified", "column"]
            lst = []

            for pair in indices:

                original = df1.iloc[pair[0], pair[1]]
                modified = df2.iloc[pair[0], pair[1]]
                column = df1.columns[pair[1]]

                lst.append([original, modified, column])

            provenance_df = pd.DataFrame(lst, columns=cols)
            provenance_df.to_csv("provenance.txt", index=False, na_rep="NA")

        return(indices)


def validate_on_unequal_cells(indices_list, changed_df):
    '''
    Loop over list of row-column pairs and run the change checker on each

    Input:
        indices_list: array of the row indices and column indices
        changed_df: the df to run the change checker on
    Output:
        valid: boolean on whether the df passed the change checker
    '''

    valid = []
    for pair in indices_list:
        valid.append(change_checker(
            changed_df.iloc[pair[0], pair[1]],
            changed_df.columns[pair[1]]
            ))

    if all(valid):
        return True
    else:
        print("The following changes don't seem to be valid for this data:")
        for x in range(len(valid)):
            if valid[x] is False:
                print("")
                print("Row {}, Column {}, \"{}\"".format(
                    indices_list[x][0]+1,
                    indices_list[x][1]+1,
                    changed_df.iloc[indices_list[x][0], indices_list[x][1]]
                    ))
                print(ERROR_MESSAGES.pop(0))
        return False


if __name__ == '__main__':

    # original df
    df_original = read_flywheel_csv(sys.argv[1])
    # edited df
    df_modified = read_flywheel_csv(sys.argv[2])

    # check for equality of each cell between the original and modified
    unequal = get_unequal_cells(df_original, df_modified)
    # if any unequal, assess the validity of the modification
    res = validate_on_unequal_cells(unequal, df_modified)

    if len(ERROR_MESSAGES) is 0 and res is True:
        print("Your changes are ready to be uploaded!")

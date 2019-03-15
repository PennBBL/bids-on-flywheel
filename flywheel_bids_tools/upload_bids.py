import sys
import os
import pandas as pd
import numpy as np
import re
import flywheel
import math
import numbers
import datetime
import argparse
from flywheel_bids_tools.utils import relist_item, get_unequal_cells
from tqdm import tqdm


ERROR_MESSAGES = []


def read_flywheel_csv(fpath, required_cols=['acquisition.id']):
    '''
    Read in a CSV and also ensure it's one of ours

    Input:
        fpath: path to the file
        required_cols: list of columns to ensure csv is a flywheel query
    Output:
        df: a pandas dataframe
    '''

    df = pd.read_csv(fpath, dtype={'valid':object})

    if not all(elem in df.columns.tolist() for elem in required_cols):
        raise Exception(("It doesn't look like this csv is correctly formatted",
        " for this flywheel editing process!"))

    df = df.sort_values(by="acquisition.id")
    drop_downs = ['classification_Measurement', 'classification_Intent']
    df.loc[:, drop_downs] = df.loc[:, drop_downs].applymap(relist_item)
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

    drop_down_multi_fields = {
        'classification_measurement': ['ASL', 'B0', 'B1', 'Diffusion',
            'Fingerprinting', 'MT', 'PD', 'Perfusion', 'Spectroscopy',
            'Susceptibility', 'T1', 'T2', 'T2*', 'Velocity'],
        'classification_intent': ['Calibration', 'Fieldmap', 'Functional',
            'Localizer', 'Non-Image', 'Screenshot', 'Shim', 'Structural']
    }

    string_fields = ['acquisition.label', 'project.label', 'error_message',
        'subject.label', 'folder', 'template',
        'intendedfor', 'mod', 'path', 'rec', 'task', 'run',
        'info_bids_error_message', 'info_sequencename',
        'type']

    numeric_fields = ['info_echotime', 'info_repetitiontime']

    # try boolean drop down option
    if column.lower() in drop_down_bool.keys():
        if str(user_input).lower() == 'true' or str(user_input).lower() == 'false':
            return True
        else:
            ERROR_MESSAGES.append("This field accepts booleans, these can only be written as \"True\" or \"False\"!")
            return False

    # try drop down string option
    elif column.lower() in drop_down_single_fields.keys():
        for field, options in drop_down_single_fields.items():
            if str(user_input).lower() in options:
                return True
            else:
                continue
        ERROR_MESSAGES.append("This field must match one of the available options in the drop-down menu on the website!")
        return False

    # try drop down multi option
    elif column.lower() in drop_down_multi_fields.keys():
        if set(user_input) <= set(drop_down_multi_fields[column.lower]):
                return True
        else:
            ERROR_MESSAGES.append("This field must match one of the available options in the drop-down menu on the website!")
            return False

    # try generic string
    elif column.lower() in string_fields:
        if type(user_input) is str or math.isnan(user_input):
            return True
        else:
            ERROR_MESSAGES.append("This field only accepts strings!")
            return False

    # try generic number
    elif column.lower() in numeric_fields:
        if isinstance(user_input, numbers.Number):
            return True
        else:
            ERROR_MESSAGES.append("This field only accepts numeric types!")
            return False

    # try the filename separately
    elif column.lower() == 'filename':
        # three different types of acceptable file names
        anat1 = re.compile(
            r'^sub-(?P<subject_id>[a-zA-Z0-9]+)(_ses-(?P<session_id>[a-zA-Z0-9]+))?(_acq-(?P<acquisition_label>[a-zA-Z0-9]+))?(_ce-(?P<contrastenhanced_id>[a-zA-Z0-9]+))?(_rec-(?P<reconstruction_id>[a-zA-Z0-9]+))?(_run-(?P<run_id>[a-zA-Z0-9]+))?(_(?P<modality>[a-zA-Z0-9]+))?((?P<suffix>\.nii(\.gz)?))$'
            )

        anat2 = re.compile(
            r'^sub-(?P<subject_id>[a-zA-Z0-9]+)(_ses-(?P<session_id>[a-zA-Z0-9]+))?(_acq-(?P<acquisition_label>[a-zA-Z0-9]+))?(_ce-(?P<contrastenhanced_id>[a-zA-Z0-9]+))?(_rec-(?P<reconstruction_id>[a-zA-Z0-9]+))?(_run-(?P<run_id>[a-zA-Z0-9]+))?(_mod-(?P<modality>[a-zA-Z0-9]+))?(_(?P<suffix>[a-zA-Z0-9]+\.nii(\.gz)?))$'
            )

        func1 = re.compile(
            r'^sub-(?P<subject_id>[a-zA-Z0-9]+)(_ses-(?P<session_id>[a-zA-Z0-9]+))?(_task-(?P<task_label>[a-zA-Z0-9]+))?(_acq-(?P<acquisition_label>[a-zA-Z0-9]+))?(_ce-(?P<contrastenhanced_id>[a-zA-Z0-9]+))?(_dir-(?P<direction>[a-zA-Z0-9]+))?(_rec-(?P<reconstruction_id>[a-zA-Z0-9]+))?(_run-(?P<run_id>[a-zA-Z0-9]+))?(_echo-(?P<echo_id>[a-zA-Z0-9]+))?(_(?P<contrast_label>[a-zA-Z0-9]+))?((?P<suffix>\.nii(\.gz)?))$'
        )

        if bool(anat1.match(str(user_input))):
            return True
        elif bool(anat2.match(str(user_input))):
            return True
        elif bool(func1.match(str(user_input))):
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


def upload_to_flywheel(modified_df, change_index, client):
    '''
    If the changes are valid, upload them to flywheel
    '''

    # loop through each of the row_col indexes of changes
    for pair in tqdm(change_index, total=len(change_index)):

        # get the acquisition id
        acquisition = modified_df.loc[pair[0], 'acquisition.id']
        file_type = modified_df.loc[pair[0], 'type']

        # get the flywheel object of the acquisition
        fw_object = client.get(str(acquisition))

        # create the update dictionary
        column_list = modified_df.columns[pair[1]].split("_")
        value = modified_df.iloc[pair[0], pair[1]]
        update = create_nested_fw_dict(column_list, value)

        f = [f for f in fw_object.files if f.type == file_type][0]
        if 'info' in update.keys():
            f.update_info(update['info'])
        if 'classification' in update.keys():
            f.update_classification(update['classification'])

    return


def create_nested_fw_dict(tree_list, value):

    if tree_list:
        return {tree_list[0]: create_nested_fw_dict(tree_list[1:], value)}
    return value


def main():

    fw = flywheel.Client()

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-orig",
        help="Path to the original flywheel query CSV",
        dest="original",
        required=True
    )
    parser.add_argument(
        "-mod",
        help="Path to the modified flywheel query CSV",
        dest="modified",
        required=True
    )

    args = parser.parse_args()

    # original df
    df_original = read_flywheel_csv(args.original)
    # edited df
    df_modified = read_flywheel_csv(args.modified)

    # check for equality of each cell between the original and modified
    unequal = get_unequal_cells(df_original, df_modified)
    # if any unequal, assess the validity of the modification
    res = validate_on_unequal_cells(unequal, df_modified)

    if len(ERROR_MESSAGES) is 0 and res is True:
        print("Changes appear to be valid! Uploading...")
        upload_to_flywheel(df_modified, unequal, fw)
        print("Done!")
        sys.exit(0)
    else:
        print("Exiting...")
        sys.exit(0)


if __name__ == '__main__':
    main()

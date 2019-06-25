import sys
import os
import pandas as pd
import numpy as np
import re
import flywheel
import numbers
import datetime
import argparse
import numbers
from flywheel_bids_tools.utils import relist_item, get_unequal_cells, is_nan, read_flywheel_csv
from tqdm import tqdm
from ast import literal_eval


ERROR_MESSAGES = []
FAILS = []


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
        'valid': ['true', 'false'],
        'info_bids_ignore': ['true', 'false']
        }

    drop_down_single_fields = {
        'modality': ['', 'nan', 'mr', 'ct', 'pet', 'us', 'eeg', 'ieeg', 'x-ray',
            'ecg', 'meg', 'nirs']
        }

    drop_down_multi_fields = {
        'classification_measurement': ['nan', 'ASL', 'B0', 'B1', 'Diffusion',
            'Fingerprinting', 'MT', 'PD', 'Perfusion', 'Spectroscopy',
            'Susceptibility', 'T1', 'T2', 'T2*', 'Velocity'],

        'classification_intent': ['nan', 'Calibration', 'Fieldmap', 'Functional',
            'Localizer', 'Non-Image', 'Screenshot', 'Shim', 'Structural'],

        'classification_features': ['nan', '3D', 'Compressed-Sensing', 'Derived',
            'Eddy-Current-Corrected', 'Fieldmap-Corrected', 'Gradient-Unwarped',
            'In-Plane', 'Magnitude', 'Motion-Corrected', 'Multi-Band',
            'Multi-Echo', 'Multi-Flip', 'Multi-Shell', 'Phase',
            'Physio-Corrected', 'Quantitative', 'Steady-State']
    }

    string_fields = ['acquisition.label', 'project.label', 'error_message',
        'subject.label', 'folder', 'template',
        'intendedfor', 'mod', 'path', 'rec', 'task', 'run',
        'info_bids_error_message', 'info_sequencename',
        'info_bids_filename', 'type', 'info_bids_folder', 'info_bids_modality',
        'info_bids_path', 'info_bids_template', 'info_seriesdescription',
        'classification_custom', 'info_bids_task', 'info_bids_acq',
        'info_bids_intendedfor', 'session.label', 'subject.label'
        ]

    numeric_fields = ['info_echotime', 'info_repetitiontime', 'info_echotime1', 'info_echotime2']

    # na is acceptible by default
    if is_nan(user_input):
        return True
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
        ERROR_MESSAGES.append("Items in this field must exactly match one of the available options in the drop-down menu on the website!")
        return False

    # try drop down multi option
    elif column.lower() in drop_down_multi_fields.keys():

        if not isinstance(user_input, list):
            user_input = [user_input]
        if set(user_input) <= set(drop_down_multi_fields[column.lower()]):
            return True
        else:
            ERROR_MESSAGES.append("This field must match one of the available options in the drop-down menu on the website!")
            return False

    # try generic string
    elif column.lower() in string_fields:
        if type(user_input) is str or is_nan(user_input):
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
        print("Warning: Column {0} not recognised!".format(column))
        return True


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
    global FAILS
    # loop through each of the modified rows
    for index, row in tqdm(modified_df.iterrows(), total=modified_df.shape[0]):

        # get the acquisition
        acquisition = row['acquisition.id']
        file_type = row['type']
        file_name = row['name']
        modality = row['modality']
        # get the flywheel object of the acquisition
        try:
            fw_object = client.get(str(acquisition))
            f = [f.to_dict() for f in fw_object.files if f.name == file_name][0]
        except Exception as e:
            print("Error fetching files for acquisition!")
            print(e)
            FAILS.append(row)
            continue

        # create MR classifier dict
        classification_vals = row.filter(regex=r"classification")
        keys = [re.sub("classification_", "", x) for x in classification_vals.index.to_list()]
        values = [None if is_nan(x) else literal_eval(x) for x in list(classification_vals.values)]
        classification = dict(zip(keys,values))
        new_class = {k: v for k, v in classification.items() if v is not None}
        current_class = f['classification']
        if new_class != current_class:
            current_class.update(new_class)

            try:
                update_attempt_classifier = fw_object.replace_file_classification(
                    f['name'],
                    current_class,
                    modality
                )

            except Exception as e:
                print("Couldn't make this classification change: Subj{}-Sess{}".format(row['subject.label'], row['session.label']))
                print(e)
                FAILS.append(row)

        # create BIDS info dict
        bids_cols = row.filter(regex=r"info_BIDS")
        bids = bids_cols.to_dict()
        bids = {re.sub("info_BIDS_", "", k): v for k, v in bids.items()}
        bids = {k: (v if not is_nan(v) else '') for k, v in bids.items()}
        del bids['info_BIDS']
        current_bids = f['info']['BIDS']

        if current_bids != bids:
            try:
                update_attempt_bids = fw_object.update_file_info(
                    f['name'],
                    {'BIDS': bids}
                )
            except Exception as e:
                print("Couldn't make this BIDS change: Subj {}-Sess {}-File {}".format(row['subject.label'], row['session.label'], row['name']))
                print(bids)
                print(e)
                FAILS.append(row)

        # create remaining info dict
        cols = row.filter(regex=r"info_(?!BIDS)")
        info = cols.to_dict()
        info = {re.sub("info_", "", k): v for k, v in info.items()}
        info = {k: (v if not is_nan(v) else '') for k, v in info.items()}
        current_info = f['info']
        del current_info['BIDS']

        if current_info != info:
            try:
                update_attempt_bids = fw_object.update_file_info(
                    f['name'],
                    info
                )
            except Exception as e:
                print("Couldn't make this info change: Subj_{}-Sess_{}-File_{}".format(row['subject.label'], row['session.label'], row['name']))
                print(bids)
                print(e)
                FAILS.append(row)


    if len(FAILS) > 0:
        fails_df = pd.concat(FAILS, sort=False)
        fails_df.to_csv("./failed_to_upload.csv", index=False)
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
        diff = df_modified.fillna(9999) != df_original.fillna(9999)
        upload_to_flywheel(df_modified.loc[diff.any(axis=1),], unequal, fw)
        print("Done!")
        sys.exit(0)
    else:
        print("Exiting...")
        sys.exit(0)


if __name__ == '__main__':
    main()

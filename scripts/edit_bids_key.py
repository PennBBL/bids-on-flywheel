import flywheel
import pandas as pd
import sys
import re


def read_flywheel_csv(fpath, required_cols=['acquisition.label',
    'valid', 'acquisition.id', 'project.label', 'session.label',
    'subject.label', 'Filename', 'Folder', 'IntendedFor', 'Mod',
    'Modality', 'Path', 'Rec', 'Run', 'Task', 'error_message',
    'ignore', 'template']):
    '''
    Read in a CSV and also ensure it's one of ours

    Input:
        fpath: path to the file
        required_cols: list of columns to ensure data comes from a fw query
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
    When a user changes a value in a column, check that the input they
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
        'subject.label', 'Filename', 'Folder', 'template',
        'IntendedFor', 'Mod', 'Path', 'Rec', 'Run', 'Task']

    user_input = str(user_input).lower()

    # try boolean drop down option
    if column.lower() in drop_down_bool.keys():
        if user_input == 'true' | user_input == 'false':
            return True
        else:
            return False

    # try drop down string option
    elif column.lower() in drop_down_single_fields.keys():
        for field, options in drop_down_single_fields.iteritems():
            if user_input in options:
                return True
            else:
                continue
        return False

    # try generic string
    elif column.lower() in string_fields:
        if isinstance(user_input, basestring):
            return True
        else:
            return False

    # try the session.label separately
    elif column == 'session.label':
        expr = re.compile(
            '^sub-(?P<subject_id>[a-zA-Z0-9]+)(_ses-(?P<session_id>[a-zA-Z0-9]+))?'
            '(_task-(?P<task_id>[a-zA-Z0-9]+))?(_acq-(?P<acq_id>[a-zA-Z0-9]+))?'
            '(_rec-(?P<rec_id>[a-zA-Z0-9]+))?(_run-(?P<run_id>[a-zA-Z0-9]+))?')
        if bool(expr.match(user_input)):
            return True
        else:
            return False

    # can't edit acquisition ID tho!
    elif column == 'acquisition.id':
        return False
    else:
        raise Exception("Column not recognised!")

if __name__ == '__main__':

    fw = flywheel.Client()
    assert fw

# original df
df = read_flywheel_csv("/home/ttapera/bids-on-flywheel/data/reward_audit.csv")
#edited df
df2 = df.copy()

df2.loc[1, 'acquisition.label'] = "a new scan"  # string field
df2.loc[1, 'valid'] = "TRUE"  # boolean field
df2.loc[1, 'Modality'] = 'MR'  # drop down choice field
df2.loc[4, 'Task'] = 89  # invalid string field
df2.loc[4, 'valid'] = 'T'  # invalid boolean field
df2.loc[4, 'Modality'] = 'magnetic resonance image'  # invalid drop down choice field

# compare the values of each, find where they are NOT the same
comparison_array = df.values == df2.values

df2.mask(comparison_array == False, )
# at each False, do a value check
r = df2.loc[1, ]
for x in r.index:
    print(r[x])
for x in r:
    print x

for ind, row in df2.itterows():

    if any(comparison_array.loc[ind, ]) is False:
        for value in row.index:

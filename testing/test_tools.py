import os
import sys
import pytest
import warnings
import flywheel
sys.path.append("..")
from flywheel_bids_tools import utils, query_bids, upload_bids
from tqdm import tqdm
import pandas as pd

'''
==========={flywheel-bids-tools testing suite}===========
=========================================================
Step 1. query-bids
=========================================================
'''


'''
1. Log in to flywheel
'''
@pytest.fixture()
def fw():
    #with warnings.catch_warnings():
    #    warnings.simplefilter("ignore")
    fw = flywheel.Client()
        #assert fw is not None
    return fw


def test_login(fw):
    assert fw is not None

'''
2. Query the server
'''

@pytest.fixture()
def query(fw, project="gear_testing"):

    result = query_bids.query_fw(project, fw)
    view = fw.View(columns='subject')
    subject_df = fw.read_view_dataframe(view, result.id)
    return subject_df

def test_query(query):

    assert query is not None
    assert query.shape[0] > 0

'''
3. Collect acquisition data
'''

@pytest.fixture()
def process_acquisitions(query, fw):

    sessions = []
    view = fw.View(columns='acquisition')
    for ind, row in tqdm(query.iterrows(), total=query.shape[0]):
        session = fw.read_view_dataframe(view, row["subject.id"])
        if session.shape[0] > 0:
            sessions.append(session)

    acquisitions = pd.concat(sessions)
    return acquisitions

def test_process_acquisisions(process_acquisitions):

    assert process_acquisitions is not None
    assert process_acquisitions.shape[0] > 0

'''
4. Collect classifier and bids data
'''

@pytest.fixture()
def get_mr_get_bids(process_acquisitions, fw):

    bids_classifications = []
    for index, row in tqdm(process_acquisitions.iterrows(), total=process_acquisitions.shape[0]):
        try:
            temp = query_bids.process_acquisition(row["acquisition.id"], fw)
            bids_classifications.append(temp)
        except:
            global UNCLASSIFIED
            UNCLASSIFIED += 1
            continue
    return bids_classifications

def test_get_mr_get_bids(get_mr_get_bids):

    assert get_mr_get_bids is not None
    assert len(get_mr_get_bids) > 0

'''
5. Tidy dataframe and write to file
'''

@pytest.fixture()
def tidy_classifications(get_mr_get_bids, process_acquisitions):

    bids_classifications = pd.concat(get_mr_get_bids, sort=True)
    merged_data = pd.merge(process_acquisitions, bids_classifications, how='outer')
    merged_data = merged_data.drop(columns=['acquisition.timestamp', 'acquisition.timezone', 'project.id', 'session.id', 'subject.id'])
    merged_data = merged_data.sort_values(by=["acquisition.id", "acquisition.label", "type"])
    infer_type = lambda x: pd.api.types.infer_dtype(x, skipna=True)
    list_cols = merged_data.apply(infer_type, axis=0) == 'mixed'
    merged_data.loc[:, list_cols] = merged_data.loc[:, list_cols].applymap(utils.unlist_item)
    merged_data.to_csv("../data/Testing/test_query.csv", index=False)
    return merged_data

def test_tidy_classifications(tidy_classifications):

    assert tidy_classifications is not None
    assert tidy_classifications.shape == (55, 30)
    assert os.path.isfile("../data/Testing/test_query.csv")


'''
=========================================================
Step 2. group-query
=========================================================
'''

'''
1. Read in
'''
@pytest.fixture()
def read_in1():

    df = utils.read_flywheel_csv("../data/Testing/test_query.csv")
    return df

def test_read_in1(read_in1):

    assert read_in1 is not None
    assert read_in1.shape[0] == 55

'''
2. group and write out
'''
@pytest.fixture()
def group_df(read_in1, groups=['info_SeriesDescription', 'type']):

    read_in1['group_id'] = (read_in1
        # groupby and keep the columns as columns
        .groupby(groups, as_index=False)
        # index the groups
        .ngroup()
        .add(1))
    read_in1 = (read_in1
        # groupby and sample 1 exemplar
        .groupby(groups, as_index=False)
        .nth(1)
        .reset_index(drop=True))
    # add index for group indeces
    read_in1['groups'] = utils.unlist_item(groups)
    read_in1 = read_in1.sort_values(by=["acquisition.id", "acquisition.label", "type"])
    read_in1.to_csv("../data/Testing/test_query_grouped.csv", index=False)
    return read_in1

def test_group_df(group_df):

    assert group_df is not None
    assert group_df.shape[0] == 15
    assert os.path.isfile("../data/Testing/test_query_grouped.csv")

'''
=========================================================
Step 3. Manipulate
=========================================================
'''

'''
=========================================================
Step 4. ungroup-query
=========================================================
'''

'''
1. Read in
'''
@pytest.fixture()
def read_in2():

    original = utils.read_flywheel_csv("../data/Testing/test_query_grouped.csv")
    modified = utils.read_flywheel_csv("../data/Testing/test_query_grouped_modified.csv")
    return original, modified

def test_read_in2(read_in2):

    assert read_in2[0] is not None
    assert read_in2[1] is not None
    assert read_in2[0].shape[0] == 15
    assert read_in2[1].shape[0] == 15


'''
2. Ungroup
'''
@pytest.fixture()
def ungroup(read_in1, read_in2):

    df_grouped_modified = read_in2[1]
    groups = utils.relist_item(df_grouped_modified['groups'][0])

    # grouped unedited
    df_grouped = read_in2[0]

    # original df
    df_original = read_in1
    # add groupings
    df_original['group_id'] = (df_original
        # groupby and keep the columns as columns
        .groupby(groups, as_index=False)
        # index the groups
        .ngroup()
        .add(1))

    # index the differences
    diff = upload_bids.get_unequal_cells(df_grouped_modified, df_grouped, provenance=True)

    changes = {}

    for x in diff:

        key = df_grouped_modified.loc[x[0], 'group_id']
        val = (df_grouped_modified.columns[x[1]], df_grouped_modified.iloc[x[0], x[1]])
        changes.update({key: val})

    for group, change in changes.items():
        df_original.loc[df_original['group_id'] == group, change[0]] = change[1]

    df_original.drop(columns='group_id', inplace=True)
    df_original = df_original.sort_values(by=["acquisition.id", "acquisition.label", "type"])
    df_original.to_csv("../data/Testing/testing_final.csv", index=False)
    return df_original

def test_ungroup(ungroup):

    assert ungroup is not None
    assert ungroup.shape[0] == 55
    assert os.path.isfile("../data/Testing/testing_final.csv")


'''
=========================================================
Step 5. upload
=========================================================
'''

'''
1. Read in
'''
@pytest.fixture()
def read_in3():

    original = upload_bids.read_flywheel_csv("../data/Testing/test_query.csv")
    modified = upload_bids.read_flywheel_csv("../data/Testing/testing_final.csv")
    return original, modified


def test_read_in3(read_in3):

    assert read_in3[0] is not None
    assert read_in3[1] is not None
    assert read_in3[0].shape == read_in3[1].shape

'''
2. Validate
'''
@pytest.fixture()
def validate(read_in3):

    # original df
    df_original = read_in3[0]
    # edited df
    df_modified = read_in3[1]

    # check for equality of each cell between the original and modified
    unequal = upload_bids.get_unequal_cells(df_original, df_modified, provenance=True)
    # if any unequal, assess the validity of the modification
    res = upload_bids.validate_on_unequal_cells(unequal, df_modified)

    return upload_bids.ERROR_MESSAGES, res, df_modified, unequal

def test_validate(validate):

    assert len(validate[0]) == 0
    assert validate[1]

'''
3. Upload
'''
@pytest.fixture()
def upload(validate, fw):

    upload_bids.upload_to_flywheel(validate[2], validate[3], fw)

    return None

def test_upload(upload):

    assert upload is None

'''
=========================================================
Step 6. Reset
=========================================================
'''

'''
1. Read in
'''
# @pytest.fixture()
# def reset_read_in3():
#
#     modified = upload_bids.read_flywheel_csv("../data/Testing/test_query.csv")
#     original = upload_bids.read_flywheel_csv("../data/Testing/testing_final.csv")
#     return original, modified
#
#
# def test_reset_read_in3(reset_read_in3):
#
#     assert reset_read_in3[0] is not None
#     assert reset_read_in3[1] is not None
#     assert reset_read_in3[0].shape == reset_read_in3[1].shape
#
# '''
# 2. Validate
# '''
# @pytest.fixture()
# def reset_validate(reset_read_in3):
#
#     # original df
#     df_original = reset_read_in3[0]
#     # edited df
#     df_modified = reset_read_in3[1]
#
#     # check for equality of each cell between the original and modified
#     unequal = upload_bids.get_unequal_cells(df_original, df_modified, provenance=True)
#     # if any unequal, assess the validity of the modification
#     res = upload_bids.validate_on_unequal_cells(unequal, df_modified)
#
#     return upload_bids.ERROR_MESSAGES, res, df_modified, unequal
#
# def test_reset_validate(reset_validate):
#
#     assert len(reset_validate[0]) == 0
#     assert reset_validate[1]
#
# '''
# 3. Upload
# '''
# @pytest.fixture()
# def reset_upload(reset_validate, fw):
#
#     upload_bids.upload_to_flywheel(reset_validate[2], reset_validate[3], fw)
#
#     return None
#
# def test_reset_upload(reset_upload):
#
#     assert reset_upload is None

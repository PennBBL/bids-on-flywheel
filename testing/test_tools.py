import os
import sys
import pytest
import warnings
import flywheel
sys.path.append("..")
from flywheel_bids_tools import utils, query_bids, upload_bids
from tqdm import tqdm
import pandas as pd
# from flywheel_bids_tools import group_query
# from flywheel_bids_tools import ungroup_query
# from flywheel_bids_tools import upload_bids
# from flywheel_bids_tools import utilities

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
    #query_result = query_bids.query_bids_validity(project, fw)
    #query_result = query_result.sort_values(by="acquisition.id")
    # query_result.to_csv(args.output, index=False)
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

    bids_classifications = pd.concat(get_mr_get_bids)
    merged_data = pd.merge(process_acquisitions, bids_classifications, how='outer')
    merged_data = merged_data.drop(columns=['acquisition.timestamp', 'acquisition.timezone', 'project.id', 'session.id', 'subject.id'])
    merged_data = merged_data.sort_values(by="acquisition.id")
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
    read_in1 = read_in1.sort_values(by="acquisition.id")
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
    diff = upload_bids.get_unequal_cells(df_grouped_modified, df_grouped)

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
    df_original = df_original.sort_values(by="acquisition.id")
    df_original.to_csv("../data/Testing/testing_final.csv", index=False)
    return df_original

def test_ungroup(ungroup):

    assert ungroup is not None
    assert ungroup.shape[0] == 55
    assert os.path.isfile("../data/Testing/testing_final.csv")

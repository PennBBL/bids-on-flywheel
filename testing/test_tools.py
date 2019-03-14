import os
import sys
import pytest
import warnings
import flywheel
sys.path.append("..")
from flywheel_bids_tools import utils, query_bids
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

# '''
# 1. Read in
# '''
# @pytest.fixture()
# def read_ungrouped():
#
#     df = utils.read_flywheel_csv("../data/Testing/test_query.csv")
#     return df
#
# def test_read_ungrouped(read_ungrouped):
#
#     assert read_ungrouped is not None
#     assert read_ungrouped.shape[0] == 55
#
# '''
# 2. group and write out
# '''
# @pytest.fixture()
# def group_df(read_grouped, groups=['info_SeriesDescription', 'type']):
#
#     read_grouped['group_id'] = (read_grouped
#         # groupby and keep the columns as columns
#         .groupby(groups, as_index=False)
#         # index the groups
#         .ngroup()
#         .add(1))
#     read_grouped = (read_grouped
#         # groupby and sample 1 exemplar
#         .groupby(groups, as_index=False)
#         .nth(1)
#         .reset_index(drop=True))
#     # add index for group indeces
#     read_grouped['groups'] = utils.unlist_item(groups)
#     read_grouped = read_grouped.sort_values(by="acquisition.id")
#     return read_grouped
#
# def test_group_df(group_df):
#
#     assert group_df is not None
#     assert read_ungrouped.shape[0] == 55

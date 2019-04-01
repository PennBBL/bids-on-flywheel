import flywheel
import pandas as pd
import sys
import argparse
from tqdm import tqdm
import re
from pandas.io.json.normalize import nested_to_record
from pandas.api.types import is_list_like
import warnings
from flywheel_bids_tools.utils import unlist_item

UNCLASSIFIED = 0
NO_DATA = 0
VERBOSE = True


def query_fw(project, client):
    """Query the flywheel client for a project name

    This function uses the flywheel API to find the first match of a project
    name. The name must be exact so make sure to type it as is on the
    website/GUI.

    Parameters
    ---------
    project
        The name of the project to search for.
    client
        The flywheel Client class object.

    Returns
    ---------
    project_object
        A flywheel project container object.
    """
    project_object = client.projects.find_first('label={0}'.format(project))

    if project_object is None:
        print("Available projects are:\n")
        for p in client.projects():
            print('%s' % (p.label))
        raise ValueError("Could not find \"{0}\" project on Flywheel!".format(project))
    return(project_object)


def extract_bids_data(acquisitionID, client):
    """Extract the BIDS data of an acquisition

    A helper function to dig into the file.info container
    (a dictionary of dictionaries) and extract the BIDS validity fields.

    Parameters
    -------
    acquisitionID
        The mongoDB hash key to identify the object.
    client
        The flywheel Client class object.

    Returns
    --------
    df
        A table of the bids fields and values.
    """
    # create the acquisition object and pull the niftis
    try:
        acq = client.get(acquisitionID)
    except:
        global NO_DATA
        NO_DATA += 1
        return None
    niftis = [x for x in acq.files if x['type'] == 'nifti']
    # if there are no niftis, return
    if (len(niftis) < 1):
        global UNCLASSIFIED
        UNCLASSIFIED += 1
        return None
    else:
        df = []
        # for each nifti, if the info has a BIDS dict:
        for nii in niftis:
            info = nii['info']
            if 'BIDS' in info.keys() and isinstance(info['BIDS'], dict):
                # also add the acquisition id to the dict for joining purposes
                nii['info']['BIDS']['acquisition.id'] = str(acquisitionID)
                # pull out the bids info
                bids = nii['info']['BIDS']
                # include TR and Series name
                bids.update({'RepetitionTime': nii['info']['RepetitionTime']})
                bids.update({'SeriesDescription': nii['info']['SeriesDescription']})
                # include the classification
                if 'classification' in nii.keys():
                    bids.update(nii.classification)
                df.append(bids)

            else:
                nii['info']['BIDS'] = {"acquisition.id": acquisitionID}
                # pull out the bids info
                bids = nii['info']['BIDS']
                # include the classification
                if 'classification' in nii.keys():
                    bids.update(nii.classification)
                df.append(bids)

        return(df)


def process_acquisition(acq_id, client):
    '''
    Extract an acquisition

    This function extracts an acquisition object and collects the important
    classification/BIDS information. These data are processed and returned as
    a pandas dataframe that can then be manipulated

    '''

    # get the acquisition object
    try:
        acq = client.get(acq_id)
        if acq is None:
            raise Exception
    except Exception as e:
        print(e)
        global UNCLASSIFIED
        UNCLASSIFIED += 1
        return pd.DataFrame({'acquisition.id': acq_id}, index = [0])
    # convert to dictionary, and flatten the dictionary to avoid nested dicts
    files = [x.to_dict() for x in acq.files]
    flat_files = [nested_to_record(my_dict, sep='_') for my_dict in files]

    # define desirable columns in regex
    cols = r'(classification)|(^type$)|(^modality$)|(BIDS)|(EchoTime)|(RepetitionTime)|(PhaseEncodingDirection)|(SequenceName)|(SeriesDescription)|(name)'

    # filter the dict keys for the columns names
    flat_files = [
        {k: v for k, v in my_dict.items() if re.search(cols, k)}
        for my_dict in flat_files
        ]

    # add acquisition ID for reference
    for x in flat_files:
        x.update({'acquisition.id': acq_id})

    # to data frame
    df = pd.DataFrame(flat_files)

    # lastly, only pull niftis and dicoms; also convert list to string
    if 'type' in df.columns:
        df = df[df.type.str.contains(r'nifti|dicom', na=False)].reset_index(drop=True)
    if 'BIDS' not in df.columns:
        global NO_DATA
        NO_DATA += 1

    return df


def query_bids_validity(project, client, VERBOSE=True):
    """Query Flywheel for BIDS data

    Main wrapper function for querying, processing, and extracting BIDS
    information for a project

    Parameters
    --------
    project
        Project to query for
    client
        A flywheel project container object
    verbose
        Print progress messages

    Returns
    --------
    merged_data
        A dataframe of the result of the query and processing
    """

    # first, log in to flywheel
    if VERBOSE:
        print("Connecting to flywheel server...")
    assert client
    # query flywheel for the argument
    if VERBOSE:
        print("Querying server...")

    result = query_fw(project, client)

    # get the subjects for this project
    view = client.View(columns='subject')
    subject_df = client.read_view_dataframe(view, result.id)

    # loop through the subjects to extract acquisitions
    if VERBOSE:
        print("Processing acquisitions...")
    sessions = []
    view = client.View(columns='acquisition')
    for ind, row in tqdm(subject_df.iterrows(), total=subject_df.shape[0]):
        session = client.read_view_dataframe(view, row["subject.id"])
        if(session.shape[0] > 0):
            sessions.append(session)

    acquisitions = pd.concat(sessions)

    # loop through the acquisitions to extract the bids validity data
    # note: speed bottleneck here
    if VERBOSE:
        print("Extracting BIDS and MR Classifier information...")
    bids_classifications = []
    for index, row in tqdm(acquisitions.iterrows(), total=acquisitions.shape[0]):

        temp = process_acquisition(row["acquisition.id"], client)
        bids_classifications.append(temp)

    bids_classifications = pd.concat(bids_classifications, sort=False)

    # finally, join the bids classification with the acquisitions
    if VERBOSE:
        print("Tidying and returning the results...")
    merged_data = pd.merge(acquisitions, bids_classifications, how='outer')
    merged_data = merged_data.drop(columns=['acquisition.timestamp', 'acquisition.timezone', 'project.id', 'session.id', 'subject.id'])
    if VERBOSE:
        print("{} acquisitions could not be processed.".format(UNCLASSIFIED))
        print("{} acquisitions do not have BIDS information yet.".format(NO_DATA))
    return(merged_data)


def main():

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        fw = flywheel.Client()
        assert fw, "Your Flywheel CLI credentials aren't set!"

    parser = argparse.ArgumentParser(description=("Use this tool to query Flywheel for a project and write out the acquisitions to a table"))
    parser.add_argument(
        "-proj", "--project",
        help="The project in flywheel to search for",
        nargs="+",
        required=True,
        dest="project"
    )
    parser.add_argument(
        "-output", "--output-file",
        help="The path and name of the output CSV of the query",
        required=True,
        dest="output"
    )
    parser.add_argument(
        "-v", "--verbose",
        help="Print out progress messages and information",
        default=True
    )

    args = parser.parse_args()

    global VERBOSE
    VERBOSE = args.verbose
    project = ' '.join(args.project)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        query_result = query_bids_validity(project, fw)
        query_result = query_result.sort_values(by=["acquisition.id", "acquisition.label"])
        drop_downs = ['classification_Measurement', 'classification_Intent',
            'classification_Features']
        query_result.loc[:, drop_downs] = query_result.loc[:, drop_downs].applymap(unlist_item)
        query_result.to_csv(args.output, index=False)
    print("Done!")


if __name__ == '__main__':
    main()

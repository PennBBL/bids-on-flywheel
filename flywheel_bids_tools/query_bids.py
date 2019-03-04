import flywheel
import pandas as pd
import sys
import argparse
from tqdm import tqdm


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
    try:
        project_object = client.projects.find_first('label={0}'.format(project))
        return(project_object)
    except:
        print("Could not find a project in flywheel with that name!")
        sys.exit(0)


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
                df.append(nii['info']['BIDS'])
            else:
                nii['info']['BIDS'] = {"acquisition.id": acquisitionID}
                df.append(nii['info']['BIDS'])

        return(df)


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
    pbar = tqdm(total=100)
    for ind, row in subject_df.iterrows():
        session = client.read_view_dataframe(view, row["subject.id"])
        if(session.shape[0] > 0):
            sessions.append(session)
        pbar.update(10)
    pbar.close()

    acquisitions = pd.concat(sessions)

    # loop through the acquisitions to extract the bids validity data
    # note: speed bottleneck here
    if VERBOSE:
        print("Extracting BIDS information...")
    bids_classifications = []
    pbar = tqdm(total=100)
    for ind, row in acquisitions.iterrows():
        temp_info = extract_bids_data(row["acquisition.id"], client)
        if temp_info is not None:
            bids_classifications.extend(temp_info)
        pbar.update(10)
    pbar.close()
    bids_classifications = pd.DataFrame(bids_classifications)

    # finally, join the bids classification with the acquisitions
    if VERBOSE:
        print("Tidying and returning the results...")
    merged_data = pd.merge(acquisitions, bids_classifications, how='outer')
    merged_data.loc[merged_data['valid'].isnull(), 'valid'] = False
    # pull relevant columns
    merged_data = merged_data[['acquisition.label', 'valid', 'acquisition.id',
    'project.label', 'session.label', 'subject.label', 'Filename', 'Folder',
    'IntendedFor', 'Mod', 'Modality', 'Path', 'Rec', 'Run', 'Task',
    'error_message', 'ignore', 'template']]

    if VERBOSE:
        print("{} acquisitions could not be processed.".format(NO_DATA))
        print("{} acquisitions do not have niftis.".format(UNCLASSIFIED))
    return(merged_data)


def main():

    fw = flywheel.Client()
    assert fw, "Your Flywheel CLI credentials aren't set!"

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "project",
        help="The project in flywheel to search for"
    )
    parser.add_argument(
        "output",
        help="The path and name of the output CSV of the query"
    )
    parser.add_argument(
        "-v",
        "--verbose",
        help="Print out progress messages and information",
        default=True
    )
    parser.add_argument(
        "-grp", "--groupings",
        nargs='+',
        dest='list',
        help="Columns to group unique rows by",
        default=None
    )
    parser.add_argument(
        "-grouped_output",
        help="The path and name of a grouped version of the output CSV of the query",
        default=None
    )

    args = parser.parse_args()
    global VERBOSE
    VERBOSE = args.verbose
    query_result = query_bids_validity(args.project, fw)
    if args.grp:
        grouped = query_result.drop_duplicates(args.grp).copy()
        grouped.to_csv(args.grouped_output, index=False)
    query_result.to_csv(args.output, index=False)


if __name__ == '__main__':
    main()

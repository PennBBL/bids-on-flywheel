import flywheel
import pandas as pd
import sys


def query_fw(project, client):
    # use the project name to query fw projects index
    try:
        project_object = client.projects.find_first('label={0}'.format(project))
        return(project_object)
    except:
        print("Could not find a project in flywheel with that name!")
        return None


def ExtractBidsValidity(acquisitionID, client, verbose=True):
    '''
    A helper function to dig into the file.info object
    (a dictionary of dictionaries) and extract the BIDS validity value
    '''
    # create the acquisition object and pull the niftis
    try:
        acq = client.get(acquisitionID)
    except:
        if verbose: print("There may not be any data for this acquisition!")
        return None
    niftis = [x for x in acq.files if x['type'] == 'nifti']
    # if there are no niftis, return
    if (len(niftis) < 1):
        if verbose: print("No BIDs classification for this acquisition yet")
        return None
    else:
        df = []
        # for each nifti, if the info has a BIDS dict:
        for nii in niftis:
            info = nii['info']
            if 'BIDS' in info.keys() and isinstance(info['BIDS'], dict):
                # also add the acquisition idto the dict for joining purposes
                nii['info']['BIDS']['acquisition.id'] = str(acquisitionID)
                # pull out the bids info
                df.append(nii['info']['BIDS'])
            else:
                return None

        return(df)


def query_bids_validity(project, client, verbose=True):

    # first, log in to flywheel
    print("Connecting to flywheel server...")
    assert client
    # query flywheel for the argument
    print("Querying server...")
    result = query_fw(project, client)

    # get the subjects for this project
    view = client.View(columns='subject')
    subject_df = client.read_view_dataframe(view, result.id)

    # loop through the subjects to extract acquisitions
    print("Processing acquisitions...")
    sessions = []
    view = client.View(columns='acquisition')
    for ind, row in subject_df.iterrows():
        session = client.read_view_dataframe(view, row["subject.id"])
        if(session.shape[0] > 0):
            sessions.append(session)

    acquisitions = pd.concat(sessions)
    # loop through the acquisitions to extract the bids validity data
    # note: speed bottleneck here
    print("Extracting BIDS information...")
    bids_classifications = []
    for ind, row in acquisitions.iterrows():
        temp_info = ExtractBidsValidity(row["acquisition.id"], client, verbose)
        if temp_info is not None:
            bids_classifications.extend(temp_info)
    bids_classifications = pd.DataFrame(bids_classifications)
    # finally, join the bids classification with the acquisitions
    print("Tidying and returning the results...")
    merged_data = pd.merge(acquisitions, bids_classifications, how='outer')
    merged_data['valid'][merged_data['valid'].isnull()] = False
    # pull relevant columns
    merged_data = merged_data[['acquisition.label', 'valid', 'acquisition.id',
    'project.label', 'session.label', 'subject.label', 'Filename', 'Folder',
    'IntendedFor', 'Mod', 'Modality', 'Path', 'Rec', 'Run', 'Task',
    'error_message', 'ignore', 'template']]

    return(merged_data)


if __name__ == '__main__':

    fw = flywheel.Client()
    assert fw

    query_result = query_bids_validity(sys.argv[1], fw)
    pd.write_csv(query_result, sys.argv[2])
# to do:

## parse argv
## make interactive help

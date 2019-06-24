import flywheel
import pandas as pd
import sys
import argparse
from tqdm import tqdm
import re
from pandas.io.json.normalize import nested_to_record
from pandas.api.types import is_list_like
import warnings
from flywheel_bids_tools.utils import unlist_item, is_list_column

UNCLASSIFIED = 0
NO_DATA = 0
VERBOSE = True


def query_fw(client, project, subject=None, session=None):
    """Query the flywheel client for a project name
    This function uses the flywheel API to find the first match of a project
    name. The name must be exact so make sure to type it as is on the
    website/GUI.
    Parameters
    ---------
    client
        The flywheel Client class object.
    project
        The name of the project to search for.
    subject
        Subject ID
    session
        Session ID

    Returns
    ---------
    seq_infos
        A list of SeqInfo objects
    """
    # first, log in to flywheel
    if VERBOSE:
        print("Connecting to flywheel server...")
    assert client
    # query flywheel for the argument
    if VERBOSE:
        print("Querying server...")

    project_object = client.projects.find_first('label={0}'.format(project))

    if project_object is None:
        print("Available projects are:\n")
        for p in client.projects():
            print('%s' % (p.label))
        raise ValueError("Could not find \"{0}\" project on Flywheel!".format(project))

    if subject is not None:
        subject = project_object.subjects.find_one('code="{}"'.format(subject))
        sessions = subject.sessions()
    elif session is not None:
        sessions = project_object.sessions.find('label="{}"'.format(session))
    else:
        sessions = project_object.sessions()

    acquisitions = [acq for s in sessions for acq in s.acquisitions()]

    return(acquisitions)


def process_query(client, acquisitions, target_cols=None):
    '''
    Extract an acquisition

    This function extracts an acquisition object and collects all imaging files
    and important classification/BIDS information. These data are processed and
    returned as a pandas dataframe that can then be exported

    Parameters
    --------
    client
        A flywheel connection object
    acquisitions
        A list of flywheel acquisition objects
    target_cols
        List of specific columns to return

    Returns
    --------
    return_df
        A dataframe of the result of the query and processing
    '''

    acquisitions_list = []
    for x in tqdm(acquisitions, total=len(acquisitions)):
        try:
            tempacq = client.get(x.id)
            if tempacq is None:
                raise Exception

            d = {
                'acquisition.id': x.id,
                'acquisition.label': x.label,
                'session.id': x.session,
                'session.label': client.get(x.parents.session).label,
                'subject.id': x.parents.subject,
                'subject.label': client.get(x.parents.subject).label,
                'timestamp': x.timestamp
            }

            files = tempacq.files
            files = [f.to_dict() for f in files]
            for f in files:

                f.update(d)

        except Exception as e:
            print(e)
            global NO_DATA
            NO_DATA += 1
            continue
        acquisitions_list.extend(files)

    files_list = [nested_to_record(fdict, sep="_") for fdict in acquisitions_list]

    global VERBOSE
    if VERBOSE:
        print("Tidying and returning the results...")
    # filter columns if necessary
    if not target_cols:
        cols = r'(\.label)|(\.id)|(classification)|(^type$)|(^modality$)|(BIDS)|(EchoTime)|(RepetitionTime)|(PhaseEncodingDirection)|(SequenceName)|(SeriesDescription)|(name)'

        # filter the dict keys for the columns names
        files_list = [
            {k: v for k, v in my_dict.items() if re.search(cols, k)}
            for my_dict in files_list
            ]
        return_df = pd.DataFrame(files_list)
    else:
        required_cols = ['\.id', '\.label', 'name']

        target_cols.extend(required_cols)
        target_cols = "|".join(["({})".format(x) for x in target_cols])

        files_list = [
            {k: v for k, v in my_dict.items() if re.search(target_cols, k)}
            for my_dict in files_list
            ]
        return_df = pd.DataFrame(files_list)

    #drop_downs = return_df.apply(is_list_column, 0, reduce=None).values
    #return_df.loc[:, drop_downs] = return_df.loc[:, drop_downs].applymap(unlist_item)
    if 'type' in return_df.columns:
        return_df = return_df[return_df.type.str.contains(r'nifti|dicom', na=False)].reset_index(drop=True)
    return(return_df)


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
        "--subject",
        help="The subject label(s)",
        nargs="+",
        default=None
    )
    parser.add_argument(
        "--session",
        help="The session label(s)",
        nargs="+",
        default=None
    )
    parser.add_argument(
        "--target_cols",
        help="List of specific columns to return",
        nargs="+",
        default=None
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
        query_result = query_fw(fw, project)
        query_result_files = process_query(fw, query_result, args.target_cols)

        if VERBOSE:
            global NO_DATA
            print("{} acquisitions could not be processed.".format(NO_DATA))

        df = query_result_files.reindex(sorted(query_result_files.columns), axis=1)
        df = df.sort_values(by=['acquisition.id', 'acquisition.label', 'name'], ascending=False).reset_index(drop=True)
        df.to_csv(args.output, index=False)
    print("Done!")


if __name__ == '__main__':
    main()

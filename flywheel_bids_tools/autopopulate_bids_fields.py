import pandas as pd
import flywheel
import warnings
import argparse
import ast
import os
from flywheel_bids_tools.query_bids import process_acquisition
#from flywheel_bids_tools.bids_generator import BidsGenerator
from flywheel_bids_tools.utils import read_flywheel_csv
from tqdm import tqdm
FAILS = []


def build_intention_path(row):

    path = "ses-{0}/{1}/{2}".format(
        row['session.label'], row['info_BIDS_Folder'], row['info_BIDS_Filename'])
    return path

def update_intentions(df, client):

    global FAILS
    df = df.dropna(subset=["info_BIDS_IntendedFor"]).reset_index()

    counter = []
    for index, row in tqdm(df.iterrows(), total=df.shape[0]):

        try:
            acq = client.get(row['acquisition.id'])
            session = client.get(acq['parents']['session'])
            acqs_df = []
            for acquisition in session.acquisitions():
                temp = process_acquisition(acquisition.id, client, target_cols=['info_SeriesDescription','info_ShimSetting', 'info_BIDS_Folder', 'info_BIDS_Filename', 'type'])
                temp['session.label'] = row['session.label']
                temp['subject.label'] = row['subject.label']
                acqs_df.append(temp)

            acqs_df = pd.concat(acqs_df, ignore_index=True, sort=False)
            acqs_df = acqs_df.loc[acqs_df.type.str.contains("nifti"),]
            current_shim = tuple(acqs_df.loc[(acqs_df['info_SeriesDescription'] == row['info_SeriesDescription']) & (acqs_df['acquisition.id'] == row['acquisition.id'])].info_ShimSetting.values[0])

            assert len(current_shim) > 0, "No shim settings for this file"

            acqs_df = acqs_df.loc[~(acqs_df['acquisition.id'] == row['acquisition.id'])]
            #acqs_df = acqs_df.dropna(subset=['info_ShimSetting'])
            #acqs_df['info_ShimSetting'] = acqs_df['info_ShimSetting'].map(tuple)
            #final_files = acqs_df.loc[(acqs_df['info_ShimSetting'] == current_shim)]
            #final_files = final_files.dropna()
            intent = [x['Folder'] for x in ast.literal_eval(row['info_BIDS_IntendedFor'])]
            final_files = acqs_df.loc[acqs_df['info_BIDS_Folder'].isin(intent), ]
            assert len(final_files) > 0, "No matching files for this shim setting"

            result = final_files.apply(build_intention_path, axis=1)
            #print("{}: This file has {} matching files".format(row['info_BIDS_Filename'], len(result)))
            acq.update_file_info(row['name'], {'IntendedFor': list(result.values)})
            counter.append(pd.DataFrame({'files': result, 'origin': row['info_BIDS_Filename']}))
        except Exception as e:
            print("Unable to update intentions for this file:")
            print(row['name'], row['session.label'], row['info_BIDS_Filename'])
            print(e)
            FAILS.append(row)

    cwd = os.getcwd()

    counter = pd.concat(counter, ignore_index=True, sort=False)
    counter.to_csv("{}/successful_intention_updates.csv".format(cwd), index=False)

    if len(FAILS) > 0:
        fails_dict = [x.to_dict() for x in FAILS]
        fails_df = pd.DataFrame(fails_dict)
        fails_df.to_csv("{}/failed_to_update_intentions.csv".format(cwd), index=False)

def update_echo_times(df, client):

    global FAILS
    df = df.dropna(subset=["info_EchoTime1"]).reset_index()

    counter = []
    for index, row in tqdm(df.iterrows(), total=df.shape[0]):

        try:
            acq = client.get(row['acquisition.id'])
            file_name = row['name']
            f = [f for f in acq.files if f.name == file_name][0]
            success = f.update_info({"EchoTime1": row["info_EchoTime1"], "EchoTime2": row["info_EchoTime2"]})
            if success:
                counter.append(row)
        except Exception as e:
            print("Unable to update echo times for this file:")
            print(row['name'], row['session.label'], row['info_BIDS_Filename'])
            print(e)
            FAILS.append(row)

    cwd = os.getcwd()

    counter = pd.concat(counter, ignore_index=True, sort=False)
    counter.to_csv("{}/successful_echotime_updates.csv".format(cwd), index=False)

    if len(FAILS) > 0:
        fails_dict = [x.to_dict() for x in FAILS]
        fails_df = pd.DataFrame(fails_dict)
        fails_df.to_csv("{}/failed_to_update_echotimes.csv".format(cwd), index=False)

def main():

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        fw = flywheel.Client()
        assert fw, "Your Flywheel CLI credentials aren't set!"

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-input",
        help="Path to the flywheel query CSV",
        dest="df",
        required=True
    )
    parser.add_argument(
        "-intentions",
        help="Update files' \"Intended For\" fields [default=True]",
        dest="intentions",
        required=False,
        default=True
    )

    args = parser.parse_args()

    # original df
    intentions_df = read_flywheel_csv(args.df, required_cols=['acquisition.id', 'info_BIDS_IntendedFor'])

    update_intentions(intentions_df, fw)
    update_echo_times(intentions_df, fw)
    print("Done!")


if __name__ == '__main__':
    main()

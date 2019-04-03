import pandas as pd
import flywheel
import warnings
import argparse

from flywheel_bids_tools.bids_generator import BidsGenerator
from flywheel_bids_tools.utils import read_flywheel_csv
from tqdm import tqdm
FAILS = []


def update_intentions(df, client):

    global FAILS
    df = df.dropna(subset=["info_BIDS_IntendedFor"])

    for index, row in tqdm(df.iterrows(), total=df.shape[0]):
        # create BIDSGenerator object to edit "intended for"
        try:
            gen = BidsGenerator()
            gen.parse_row(row)
            gen.update_intention(client)
            #if result is None:
            #    raise AssertionError
        except Exception as e:
            print("Unable to update intentions for this file:")
            print(row['name'], row['session.label'])
            print(e)
            FAILS.append(row)

    if len(FAILS) > 0:
        fails_df = pd.concat(FAILS, sort=False)
        fails_df.to_csv("./failed_to_update_intentions.csv", index=False)


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
    print("Done!")


if __name__ == '__main__':
    main()

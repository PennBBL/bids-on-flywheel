import json
import argparse
import flywheel
import warnings

def process_config(config):
    '''
    Process the config file to determine the inputs and outputs
    '''

    if config['name'] == "dicom-mr-classifier":

        input_1 = "acquisition.id"
    inputs = config['inputs']
    print(list(inputs.keys()))
    print('base' in list(inputs.keys()))


def main():

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        fw = flywheel.Client()
        assert fw, "Your Flywheel CLI credentials aren't set!"

    parser = argparse.ArgumentParser(description=("Use this tool to run a Flywheel gear on the changes you've made."))

    parser.add_argument(
        "-config", "--config-file",
        dest='config',
        help="Path to the config.json file from a gear query",
        required=True
    )
    parser.add_argument(
        "-orig",
        help="Path to the original flywheel query CSV",
        dest="original",
        required=True
    )
    parser.add_argument(
        "-mod",
        help="Path to the modified flywheel query CSV",
        dest="modified",
        required=True
    )

    args = parser.parse_args()

    with open(args.config) as f:
        config = json.load(f)

    process_config(config)

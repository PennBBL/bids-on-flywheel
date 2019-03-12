import pandas as pd
import flywheel
import warnings
import argparse
from tabulate import tabulate
from pandas.io.json.normalize import nested_to_record
import json
import sys


def find_gear(gear_name, client):
    '''
    Queries flywheel for a gear by name
    '''

    try:
        gear = client.lookup('gears/{}'.format(gear_name))
        return gear
    except:
        print("No gear found by that name. Use \"query-gears -name all\" to see all the gears available to you")
        SystemExit(0)


def collect_gear_config(gear_id, client):
    '''
    Collects the gear's configuration and inputs
    '''
    gear = client.get_gear(gear_id)
    name = gear['gear']['name']
    label = gear['gear']['label']
    description = gear['gear']['description']
    inputs = nested_to_record(gear.gear.inputs)
    config = nested_to_record(gear.get_default_config())
    return({'name': name, 'inputs': inputs, 'config': config, 'label': label, 'description': description})


def str2bool(v):
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')


def main():

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        fw = flywheel.Client()
        assert fw, "Your Flywheel CLI credentials aren't set!"

    parser = argparse.ArgumentParser(description=("Use this to query Flywheel for the gears available to you, or get the config file for a gear."))

    parser.add_argument(
        "-name", "--gear-name",
        dest='name',
        help="Shorthand name of the gear on Flywheel",
        required=True,
        default='all'
    )
    parser.add_argument(
        "-config", "--output-config",
        dest='config',
        help="True/False; Whether to output configuration file for running",
        required=False,
        default='False'
    )

    args = parser.parse_args()

    config = str2bool(args.config)

    if args.name == 'all':
        gears = fw.gears()
        gears_table = [nested_to_record(g.to_dict(), sep='_') for g in gears]
        df = pd.DataFrame(gears_table)
        df = df.filter(regex=r'gear_label$|gear_name$|^category$', axis = 1)
        print(tabulate(df, headers='keys', tablefmt='psql',))

    else:
        gear = find_gear(args.name, fw)
        config_file = collect_gear_config(gear['_id'], fw)
        if config:
            with open('gear_config.json', 'w') as outfile:
                json.dump(config_file, outfile)
            print("Config file written.")
        else:
            print(json.dumps(config_file, indent=4))

if __name__ == '__main__':
    main()

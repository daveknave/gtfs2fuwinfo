import yaml
import pandas as pd
import os, io

def merge_output_data(config):
    files = {
        '$STOPPOINTS:':     'stoppoints.txt',
        '$LINE:':           'line.txt',
        '$SERVICEJOURNEY:': 'servicejourney.txt',
        '$DEADRUNTIMES:':   'deadruntime.txt',
        '$CONNECTIONS:':    'connections.txt',
    }

    statistics = {}

    with open(os.path.join(config['out_directory'], f'output.txt'), 'w', newline='', encoding='utf-8') as fh:
        for tab_name, file_name in files.items():
            tmp_df = pd.read_csv(os.path.join(config['out_directory'], file_name), sep=';', encoding='utf-8')
            statistics[tab_name] = tmp_df.shape[0]

            str_buff = io.StringIO()
            tmp_df.to_csv(str_buff, index=False, sep=';', encoding='utf-8')

            out_str = '''*
*
*
'''
            out_str += tab_name
            out_str += str_buff.getvalue()

            fh.write(out_str)

    informative_file_name = f"{config['agency']}_{statistics['$SERVICEJOURNEY:']}_{statistics['$LINE:']}_{statistics['$STOPPOINTS:']}.txt"

    ### Remove old output file
    if os.path.exists(os.path.join(config['out_directory'], informative_file_name)):
        os.remove(os.path.join(config['out_directory'], informative_file_name))

    ### Rename output file
    os.rename(
        os.path.join(config['out_directory'], f'output.txt'),
        os.path.join(config['out_directory'], informative_file_name)
    )

if __name__ == '__main__':
    with open('../config.yaml', 'r') as fh:
        config = yaml.load(fh, Loader=yaml.FullLoader)

    merge_output_data(config)

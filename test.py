import math
import os
from itertools import combinations

import numpy as np
import yaml

import pandas as pd
import networkx as nx

with open('config.yaml', 'r') as fh:
    config = yaml.load(fh, Loader=yaml.FullLoader)

# 10801
input_tables = {}
for file_name in os.listdir(config['in_directory']):
    if ".txt" not in file_name:
        continue
    input_tables[file_name] = pd.read_csv(
        os.path.join(config['in_directory'], file_name),
        delimiter=",",
        decimal=".",
        quotechar='"',
    )

depots = pd.read_csv(os.path.join(config['in_directory'], "vbb_depots.csv"), sep=";", decimal=",")
#%%
shapes_df = input_tables['shapes.txt']

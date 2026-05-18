### pargroupby.py
### Minimal extension to perform parallel group by on pandas DataFrames
### by daveknave
import pandas as pd
import multiprocessing as mp
import math
import time, sys

out_li = []

finished_groups = mp.Value('i', 0)
total_groups = 0

def append_result(res_):
    global finished_groups, out_li
    finished_groups.value += 1
    percent = finished_groups.value/total_groups * 100.0

    sys.stdout.write(str(math.floor(percent)) + ' % \r')
    sys.stdout.flush()

    out_li.append(res_)

def error_occurred(e):
    print("-->{}<--".format(e))
    raise(e)

# parralel groupby
def do(gr, func, name = 'Multi Process', ncores = mp.cpu_count, args_dict = {}):
    global total_groups, out_li
    out_li = []

    print('Parellel group apply started ...')
    pool = mp.Pool(ncores)
    pool.name = name
    total_groups = len(gr.groups)
    for g in gr.groups:
        args_dict.update({'index' : g})
        res_ = pool.apply_async(func, [gr.get_group(g)], args_dict, append_result, error_occurred)

    pool.close()
    pool.join()
    return pd.DataFrame(data=out_li)

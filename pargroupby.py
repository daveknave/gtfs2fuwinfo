### pargroupby.py
### Minimal extension to perform parallel group by on pandas DataFrames
### by daveknave
import pandas as pd
import multiprocessing as mp
import time

out_li = []

def append_result(res_):
    out_li.append(res_)

# parralel groupby
def do(gr, func, name = 'Multi Process', ncores = 1, args_dict = {}):
    pool = mp.Pool(ncores)
    pool.name = name
    print(gr.groups)
    for g in gr.groups:
        res_ = pool.apply_async(func, [gr.get_group(g), g], args_dict, append_result)

    pool.close()
    pool.join()
    return(pd.DataFrame(data=out_li))
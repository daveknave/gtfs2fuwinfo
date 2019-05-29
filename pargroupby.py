### pargroupby.py
### by daveknave
import pandas as pd
import multiprocessing as mp

# parralel groupby
def pargroupby(gr, func, name = 'Multi Process', ncores = 1, args_dict = {}):
    pool = mp.Pool(ncores)
    pool.name = name
    out_li = []
    for g in gr.groups:
        res_ = pool.apply_async(func, [gr.get_group(g), g], args_dict).get()
        out_li.extend(res_)

    pool.close()
    pool.join()
    return(pd.DataFrame(out_li))
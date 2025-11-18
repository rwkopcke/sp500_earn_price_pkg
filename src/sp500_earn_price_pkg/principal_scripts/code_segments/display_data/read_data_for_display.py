import json
import polars as pl
import polars.selectors as cs

import config.config_paths as config
import config.set_params as params

from sp500_earn_price_pkg.helper_func_module \
    import helper_func as hp

env = config.Fixed_locations()
param = params.Display_param()


def history(record_dict):
    list_ = [item[7:17] 
             for item in record_dict['prev_used_files']]
    #yr_qtr_set = set(hp.date_to_year_qtr(list_).to_list())
    
    if env.OUTPUT_HIST_ADDR.exists():
        with env.OUTPUT_HIST_ADDR.open('r') as f:
            data_df = pl.read_parquet(source= f,
                                      columns= param.HIST_COL_NAMES)
        hp.message([
            f'Read data history from: \n{env.OUTPUT_HIST_ADDR}'
        ])
    else:
        hp.message([
            f'No data history in: \n{env.OUTPUT_HIST_ADDR.name}',
            f'at: \n{env.OUTPUT_HIST_ADDR}',
            'Processing ended'
        ])
        quit()
        
    return data_df  #, sorted(yr_qtr_set)


def projection():
    proj_df = pl.DataFrame()
    if env.OUTPUT_PROJ_ADDR.exists():
        with env.OUTPUT_PROJ_ADDR.open('r') as f:
            proj_df = pl.read_parquet(source= f)
        
    if not proj_df.is_empty():
        hp.message([
            f'Read projection dataframe',
            f'at \n{env.OUTPUT_PROJ_ADDR}'
        ])
        
        
        proj_dict = dict()
        for k in proj_df.columns:
            proj_dict[k] = proj_df.select(pl.col(k)
                                          .struct.unnest())\
                                  .drop_nulls()\
                                  .cast({cs.float(): pl.Float32,
                                         cs.integer(): pl.Int16})\
                                  .filter(pl.col('yr_qtr')>=k)
    else:
        hp.message([
            f'No file at \n{env.OUTPUT_PROJ_ADDR}',
            'Processing ended'
        ])
        quit()
    
    return proj_dict, sorted(set(proj_dict.keys()), reverse= True)


def record():
    if env.RECORD_DICT_ADDR.exists():
        with env.RECORD_DICT_ADDR.open('r') as f:
            record_dict = json.load(f)
        hp.message([
            f'Read record_dict from: \n{env.RECORD_DICT_ADDR}'
        ])
    else:
        hp.message([
            f'No record_dict in \n{env.RECORD_DICT_ADDR.name}',
            f'at: \n{env.RECORD_DICT_ADDR}',
            'Processing ended'
        ])
        quit()
        
    # returns proj's date
    date_this_projn = record_dict['latest_file'][7:17]
    # returns yr_qrt, a pl.series with one str element
    # then, extract the str
    yr_qtr_current_projn = \
        hp.date_to_year_qtr([date_this_projn]).to_list()[0]
    
    return record_dict, date_this_projn, yr_qtr_current_projn

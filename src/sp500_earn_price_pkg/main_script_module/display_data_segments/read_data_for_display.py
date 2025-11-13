import sys

import json
import polars as pl
import polars.selectors as cs

import sp500_earn_price_pkg.config.config_paths as config
import sp500_earn_price_pkg.config.set_params as param

from sp500_earn_price_pkg.helper_func_module \
    import helper_func as hp


def history(record_dict):
    yr_qtr_set = {item['yr_qtr']
                  for item in record_dict['prev_used_files']}
    
    if config.Fixed_locations().OUTPUT_HIST_ADDR.exists():
        with config.Fixed_locations().OUTPUT_HIST_ADDR.open('r') as f:
            data_df = pl.read_parquet(source= f,
                                      columns= param.Display_param().HIST_COL_NAMES)
            
        print('\n============================================')
        print(f'Read data history from: \n{config.Fixed_locations().OUTPUT_HIST_ADDR}')
        print('============================================\n')
    else:
        print('\n============================================')
        print(f'No data history in: \n{config.Fixed_locations().OUTPUT_HIST_ADDR.name}')
        print(f'at: \n{config.Fixed_locations().OUTPUT_HIST_ADDR}')
        print('Processing ended')
        print('============================================\n')
        sys.exit()
        
    return data_df, yr_qtr_set


def projection(record_dict, yr_qtr_set):
    proj_df = pl.DataFrame()
    if config.Fixed_locations().OUTPUT_PROJ_ADDR.exists():
        with config.Fixed_locations().OUTPUT_PROJ_ADDR.open('r') as f:
            proj_df = pl.read_parquet(source= f)
        
    if not proj_df.is_empty():
        print('\n============================================')
        print(f'Read projection dataframe' )
        print(f'at \n{config.Fixed_locations().OUTPUT_PROJ_ADDR}')
        print('============================================\n')
        
        proj_dict = dict()
        for k in proj_df.columns:
            proj_dict[k] = proj_df.select(pl.col(k)
                                          .struct.unnest())\
                                  .drop_nulls()\
                                  .cast({cs.float(): pl.Float32,
                                         cs.integer(): pl.Int16})\
                                  .filter(pl.col('yr_qtr')>=k)
        
    else:
        print('\n============================================')
        print(f'No file at \n{config.Fixed_locations().OUTPUT_PROJ_ADDR}')
        print('Processing ended')
        print('============================================\n')
        sys.exit()
    
    return proj_dict, sorted(set(proj_dict.keys()), reverse= True)


def record():
    if config.Fixed_locations().RECORD_DICT_ADDR.exists():
        with config.Fixed_locations().RECORD_DICT_ADDR.open('r') as f:
            record_dict = json.load(f)
        print('\n============================================')
        print(f'Read record_dict from: \n{config.Fixed_locations().RECORD_DICT_ADDR}')
        print('============================================\n')
    else:
        print('\n============================================')
        print(f'No record_dict in \n{config.Fixed_locations().RECORD_DICT_ADDR.name}')
        print(f'at: \n{config.Fixed_locations().RECORD_DICT_ADDR}')
        print('Processing ended')
        print('============================================\n')
        sys.exit()
        
    # returns proj's date, a polars series with one date element
    date_this_projn = \
        hp.string_to_date([record_dict['latest_used_file']])
    # returns yr_qrt, a polar series with one str element
    # then, extract the str
    yr_qtr_current_projn = \
        hp.date_to_year_qtr(date_this_projn).to_list()[0]
    # extract the datetime.date 
    date_this_projn = date_this_projn.to_list()[0]
    
    return record_dict, date_this_projn, yr_qtr_current_projn

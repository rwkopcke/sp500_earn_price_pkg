import sys
import polars as pl

import sp500_earn_price_pkg.config.config_paths as config
import sp500_earn_price_pkg.config.set_params as param
# param.Display_param()

def read(record_dict):
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

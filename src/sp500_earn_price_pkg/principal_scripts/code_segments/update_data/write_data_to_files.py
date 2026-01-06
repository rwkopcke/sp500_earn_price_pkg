import polars as pl
import json

import config.config_paths as config

from sp500_earn_price_pkg.helper_func_module \
    import helper_func as hp

env = config.Fixed_locations


def record(record_dict):
    with open(env.RECORD_DICT_ADDR, 'w') as f:
        json.dump(record_dict, f, indent= 4)
    if env.BACKUP_RECORD_TEMP_ADDR.exists():
        env.BACKUP_RECORD_TEMP_ADDR \
            .rename(env.BACKUP_RECORD_DICT_ADDR)
        hp.message([
                f'Moved previous record_dict to:',
                f'{env.BACKUP_RECORD_DICT_ADDR}'
        ])
    
    hp.message([
        f'Updated:\n {env.RECORD_DICT_ADDR}',
        f'\nlatest_file:{record_dict['latest_file']}\n',
        f'prev_used_files:\n{record_dict['prev_used_files'][:4]}\n',
        f'other_prev_files:\n{record_dict['other_prev_files'][:4]}'
    ])
    
    return


def history(actual_df):
    # move any existing hist file in output_dir to backup
    with env.OUTPUT_HIST_ADDR.open('wb') as f:
        actual_df.write_parquet(f)
    hp.message([f'Updated\n{env.OUTPUT_HIST_ADDR}'])
        
    if env.BACKUP_HIST_TEMP_ADDR.exists():
        env.BACKUP_HIST_TEMP_ADDR \
            .rename(env.BACKUP_HIST_ADDR)
        hp.message([
            f'Moved previous history file to:\n{env.BACKUP_HIST_ADDR}'
        ])
    
    return


def industry(ind_df):
    
    with env.OUTPUT_IND_ADDR.open('wb') as f:
        ind_df.write_parquet(f)
    hp.message([f'Updated\n{env.OUTPUT_IND_ADDR}'])
    
    if env.BACKUP_IND_TEMP_ADDR.exists():
        env.BACKUP_IND_TEMP_ADDR \
            .rename(env.BACKUP_IND_ADDR)
        hp.message([
            f'Moved previous industry file to: \n{env.BACKUP_IND_ADDR}'
        ])
    
    return


def projection(proj_dict, new_files_set):
    '''
        proj_dict: keys, year_quarter of projection
        config.Fixed_locations: provides the address for storing the data
        
        Writes the projection data for each year_quarter
        to a single parquet file. 
        proj_dict
            keys are year_quarters
            values are df with projections for future qtrs
        proj_df is the format for saving proj_dict
            cols = proj_df keys
            rows are structs, one struct for each
                future qtr in the projection
            a struct contains projections for various
                measures of E for its quarter

        To recover proj_df
            with proj_address.open('rb') as f:
                proj_df = pl.read_parquet(f)
                
        To recover proj_dict
            proj_dict = proj_df.to_dict(as_series= False)
            
        To recover each proj_date_df
            for k in proj_dict.keys():
                # for year_quarter == k
                proj_date_df = pl.DataFrame(proj_dict[k])
    '''
    
    proj_address = env.OUTPUT_PROJ_ADDR

    # convert proj_dict to df to save with parquet
    # use concat to compensate for diff # of rows in each proj_date_df
    # pads short cols with null
    # creates col for each key (k)
    
    proj_hist_df = \
        pl.concat(
            items= [pl.DataFrame({key: value})
                    for key, value in proj_dict.items()],
            how= "horizontal")
    
    with proj_address.open('wb') as f:
        proj_hist_df.write_parquet(f)
    hp.message([f'Updated\n{env.OUTPUT_PROJ_ADDR}'])
    
    if env.BACKUP_PROJ_TEMP_ADDR.exists():
        env.BACKUP_PROJ_TEMP_ADDR \
            .rename(env.BACKUP_PROJ_ADDR)
        hp.message([
            f'Moved previous industry file to: \n{env.BACKUP_PROJ_ADDR}'
        ])
        
    # new_files_set is not empty  
    for file in new_files_set:
        address = env.INPUT_DIR / file
        address.rename(env.ARCHIVE_DIR / file)
    hp.message([
        f'Archived all new files from S&P to\n{env.ARCHIVE_DIR}'])
        
    return

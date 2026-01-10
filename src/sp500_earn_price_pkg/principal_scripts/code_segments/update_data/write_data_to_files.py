import sys
import polars as pl
import json

import config.config_paths as config

from sp500_earn_price_pkg.helper_func_module \
    import helper_func as hp

env = config.Fixed_locations

def restore_temp_files(exit= True):
    '''
    Restores original parquet files and record_dict.json
    '''
    for temp, orig_file in zip([
                        env.BACKUP_RECORD_TEMP_ADDR,
                        env.BACKUP_HIST_TEMP_ADDR,
                        env.BACKUP_IND_TEMP_ADDR,
                        env.BACKUP_PROJ_TEMP_ADDR], 
                                        [
                        env.RECORD_DICT_ADDR,
                        env.OUTPUT_HIST_ADDR,
                        env.OUTPUT_IND_ADDR,
                        env.OUTPUT_PROJ_ADDR]
                    ):
        if temp.exists():
            temp.rename(orig_file)
            hp.message([
                f'Restored: \n{orig_file}',
                f'From: \n{temp}'
            ])
    if exit:
        hp.message([
            f'Processing halted'
        ])
        sys.exit()
    return


def history(actual_df):
    '''
    Writes updated actual_df to parquet file
    Writes the previous version (from temp) to backup
    '''
    # move any existing hist file in output_dir to backup
    
    write_data_file(actual_df, env.OUTPUT_HIST_ADDR)
    move_temp_to_backup(env.BACKUP_HIST_TEMP_ADDR, 
                        env.BACKUP_HIST_ADDR)
    return


def industry(ind_df):
    '''
    Writes updated ind_df to parquet file
    Writes the previous version (from temp) to backup
    '''
    write_data_file(ind_df, env.OUTPUT_IND_ADDR)
    move_temp_to_backup(env.BACKUP_IND_TEMP_ADDR, 
                        env.BACKUP_IND_ADDR)
    return


def projection(proj_dict):
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
    # convert proj_dict to df to save with parquet
    # use concat to compensate for diff # of rows in each proj_date_df
    # pads short cols with null
    # creates col for each key (k)
    
    proj_hist_df = \
        pl.concat(
            items= [pl.DataFrame({key: value})
                    for key, value in proj_dict.items()],
            how= "horizontal")

    write_data_file(proj_hist_df, env.OUTPUT_PROJ_ADDR)
    move_temp_to_backup(env.BACKUP_PROJ_TEMP_ADDR, 
                        env.BACKUP_PROJ_ADDR)
    return


def archive_sp_input_xlsx(new_files_set):
    '''
    Writes sp input xlsx files to archive
    '''
    # new_files_set is not empty  
    for file in new_files_set:
        address = env.INPUT_DIR / file
        address.rename(env.ARCHIVE_DIR / file)
    hp.message([
        f'Archived all new files from S&P to\n{env.ARCHIVE_DIR}'])
    return


def record(record_dict):
    '''
    Writes updated record_df to json file
    Writes the previous version of json (from temp) to backup
    '''
    with open(env.RECORD_DICT_ADDR, 'w') as f:
        json.dump(record_dict, f, indent= 4)
        
    move_temp_to_backup(env.BACKUP_RECORD_TEMP_ADDR, 
                        env.BACKUP_RECORD_DICT_ADDR)
    
    hp.message([
        f'Updated:\n {env.RECORD_DICT_ADDR}',
        f'\nlatest_file:{record_dict['latest_file']}\n',
        f'prev_used_files:\n{record_dict['prev_used_files'][:4]}\n',
        f'other_prev_files:\n{record_dict['other_prev_files'][:4]}'
    ])
    return


def move_temp_to_backup(temp, backup_addr):
    '''
    Moves temporary file to backup file
    if update_data.py is successful
    '''
    if temp.exists():
        temp.rename(temp)
    hp.message([
            f'Moved {temp} to: \n{backup_addr}'
        ])
    return


def write_data_file(df, file_addr):
    '''
    Updates data file
    if update_data.py is successful
    '''
    with file_addr.open('wb') as f:
        df.write_parquet(f)
    hp.message([f'Updated\n{file_addr}'])
    return

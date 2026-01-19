import sys
import polars as pl
import json

import config.config_paths as config

from sp500_earn_price_pkg.helper_func_module \
    import helper_func as hp

env = config.Fixed_locations

input_dir = env.INPUT_DIR
sp_glob_str = env.INPUT_SP_FILE_GLOB_STR


def restore_data_stop_update(location= "missing", 
                             exit= True):
    '''
    Restores original parquet files and record_dict.json
        .rename() removes TEMP files
    If exit= True, halt
    Otherwise return to processing
    '''
    hp.message([
            f'At {location}: processing was halted',
            'Data in any TEMP files have been restored to original files',
            'See the following messages'
        ])
    
    if not env.BACKUP_TEMP_DIR.exists():
        hp.message([
            'No data files restored because'
            'No temporary directory exists'
        ])
    
    else:
        for file in env.BACKUP_TEMP_DIR.iterdir():
            match str(file.name):
                case env.BACKUP_HIST_TEMP:
                    move_file_from_to(
                        env.BACKUP_HIST_TEMP_ADDR,
                        env.OUTPUT_HIST_ADDR
                    )
                case env.BACKUP_IND_TEMP:
                    move_file_from_to(
                        env.BACKUP_IND_TEMP_ADDR,
                        env.OUTPUT_IND_ADDR
                    )
                case env.BACKUP_PROJ_TEMP:
                    move_file_from_to(
                        env.BACKUP_PROJ_TEMP_ADDR,
                        env.OUTPUT_PROJ_ADDR
                    )
                case env.BACKUP_RECORD_TEMP:
                    move_file_from_to(
                        env.BACKUP_RECORD_TEMP_ADDR,
                        env.RECORD_DICT_ADDR
                    )
    if exit:
        hp.message([
            f'Processing halted'
        ])
        sys.exit()
    return


def history(actual_df):
    '''
        Writes updated actual_df to parquet file
        Writes the previous version to temp
    '''
    # move any existing hist file in output_dir to backup
    if env.OUTPUT_HIST_ADDR.exists():
        move_file_from_to(env.OUTPUT_HIST_ADDR,
                          env.BACKUP_HIST_TEMP_ADDR)
    write_data_file(actual_df, env.OUTPUT_HIST_ADDR)
    return


def industry(ind_df):
    '''
        Writes updated ind_df to parquet file
        Writes the previous version to temp
    '''
    if env.OUTPUT_IND_ADDR.exists():
        move_file_from_to(env.OUTPUT_IND_ADDR,
                      env.BACKUP_IND_TEMP_ADDR)
    write_data_file(ind_df, env.OUTPUT_IND_ADDR)
    return


def projection(proj_dict):
    ''' 
        proj_dict
            key, year_quarter
            value, df with projections for future qtrs
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
    #     this operation pads short cols with null
    # creates col for each key (k)
    
    proj_hist_df = \
        pl.concat(
            items= [pl.DataFrame({key: value})
                    for key, value in proj_dict.items()],
            how= "horizontal")
    if env.OUTPUT_PROJ_ADDR.exists():
        move_file_from_to(env.OUTPUT_PROJ_ADDR,
                      env.BACKUP_PROJ_TEMP_ADDR)
    write_data_file(proj_hist_df, env.OUTPUT_PROJ_ADDR)
    return


def record(record_dict):
    '''
        Writes updated record_df to json file
        Writes the previous version to temp
    '''
    if env.RECORD_DICT_ADDR.exists():
        move_file_from_to(env.RECORD_DICT_ADDR, 
                      env.BACKUP_RECORD_TEMP_ADDR)
    with open(env.RECORD_DICT_ADDR, 'w') as f:
        json.dump(record_dict, f, indent= 4)
    
    hp.message([
        f'Updated:\n {env.RECORD_DICT_ADDR}',
        f'\nlatest_file:{record_dict['latest_file']}\n',
        f'prev_used_files:\n{record_dict['prev_used_files'][:4]}\n',
        f'other_prev_files:\n{record_dict['other_prev_files'][:4]}'
    ])
    return


def archive_sp_input_xlsx(new_files_set):
    '''
        Writes new sp input xlsx files to archive
        Erases redundant sp input xlsx files
    '''
    # new_files_set is not empty
    for file in new_files_set:
        address = input_dir / file
        address.rename(env.ARCHIVE_DIR / file)
    hp.message([
        f'Archived all new files from S&P to\n{env.ARCHIVE_DIR}'])
    
    # erase sp files previously archived
    # new files removed/renamed above
    for file in set(str(f.name) for f in 
                    input_dir.glob(sp_glob_str)):
        (input_dir / file).unlink()
    return


def write_temp_to_backup():
    '''
        Rename temp files to backup files
    '''
    if not env.BACKUP_TEMP_DIR.exists():
        hp.message([
            'Temporary backup directory does not exist.'
        ])
        return
    
    hp.message([
            'Complete update: write temp files to backup'
        ])
    for file in env.BACKUP_TEMP_DIR.iterdir():
        match str(file.name):
            case env.BACKUP_HIST_TEMP:
                move_file_from_to(
                    env.BACKUP_HIST_TEMP_ADDR,
                    env.BACKUP_HIST_ADDR
                )
            case env.BACKUP_IND_TEMP:
                move_file_from_to(
                    env.BACKUP_IND_TEMP_ADDR,
                    env.BACKUP_IND_ADDR
                )
            case env.BACKUP_PROJ_TEMP:
                move_file_from_to(
                    env.BACKUP_PROJ_TEMP_ADDR,
                    env.BACKUP_PROJ_ADDR
                )
            case env.BACKUP_RECORD_TEMP:
                move_file_from_to(
                    env.BACKUP_RECORD_TEMP_ADDR,
                    env.BACKUP_RECORD_DICT_ADDR
                )
    
    hp.message([
        f'Files remaining in {
            str(env.BACKUP_TEMP_DIR.name)}:',
        f'{[str(f.name) for f in env.BACKUP_TEMP_DIR.iterdir()]}'
    ])
    return


def move_file_from_to(current, new):
    '''
       Renaming deletes original file
    '''
    current.rename(new)
    hp.message([
            f'Moved: \n{current} \nto: \n{new}'
        ])
    return


def write_data_file(df, file_addr):
    '''
        Updates data file with new data
    '''
    with file_addr.open('wb') as f:
        df.write_parquet(f)
    hp.message([f'Updated\n{file_addr}'])
    return

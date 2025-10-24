import polars as pl
import json

from sp500_earn_price_pkg.helper_func_module \
    import helper_func as hp
    
import sp500_earn_price_pkg.config.config_paths as config
import sp500_earn_price_pkg.config.set_params as param

env = config.Fixed_locations()


def fetch():
    '''
        Reads record_dict.json
        if it exists, writes it to backup
        
        Returns
            record_dict, if it exists or
            new blank record_dict
                RECORD_DICT_TEMPLATE
                from config_paths.py
    '''
    
# READ: record_dict
    if env.RECORD_DICT_ADDR.exists():
        with open(env.RECORD_DICT_ADDR,'r') as f:
            record_dict = json.load(f)
            
# WRITE: if prev record_dict exists, write backup
        with open(env.BACKUP_RECORD_DICT_ADDR, 'w') as f:
            json.dump(record_dict, f, indent= 4)
        hp.message([
            f'Read record_dict from: \n{env.RECORD_DICT_ADDR}',
            f'Wrote record_dict to: \n{env.BACKUP_RECORD_DICT_ADDR}'
        ])
        
    else:
        # create record_dict from keys and empty values
        record_dict = env.RECORD_DICT_TEMPLATE
        hp.message([
            f'No record_dict.json exists at',
            f'{env.RECORD_DICT_ADDR}',
            'Created new record_dict, '
        ])
        
    return record_dict


def update(record_dict, input_files_set):
    '''
        Receives 
            record_dict
            set of input files
        Creates 
            new_files_set
                set of new, previously unrecorded files
            files_to_be_read_set (subset of new_files_set)
                names of input files to read, which
                contain latest data for each quarter.
                (other files with new data are archived)
    '''
    
# CREATE set of new files
    prev_files_set = set(
        env.ARCHIVE_DIR.glob(
            env.INPUT_SP_FILE_GLOB_STR)
    )
    new_files_set = input_files_set - prev_files_set
    
# if nothing new, return
    if not new_files_set:
        return [dict(), set(), set()]

# ===========================================================
# NB from here, new_files_set is not empty
#    values for record_dict's keys may be empty
# ===========================================================
   
# UPDATE record_dict['prev_used_files'] for the new_used_files

    # create df with the names of the new_files
    # and col names that conform to the keys of the 
    # dicts in record_dict['prev_used_files']:
    # file, date and param.Update_param().YR_QTR_NAME
    data_df = pl.DataFrame(list(new_files_set), 
                           schema= ["file"],
                           orient= 'row')\
                .with_columns(pl.col.file
                        .map_batches(hp.file_to_date_str,
                                     return_dtype= pl.String)
                        .alias('date'))\
                .with_columns(pl.col.date
                        .map_batches(hp.date_to_year_qtr,
                                     return_dtype= pl.String)
                        .alias(param.Update_param().YR_QTR_NAME))
    
    # fetch prev_used_files list from record_dict
    # prev_used_files contains a list of dicts, each with keys
    # 'file', 'date', param.Update_param().YR_QTR_NAME
    # one dict for each prev_used_file
    prev_used_files_list = record_dict['prev_used_files']
    if prev_used_files_list:
        # col names are the same as the keys 
        # contained in prev_used_files_list
        used_df = pl.DataFrame(prev_used_files_list,
                               orient= 'row')
        data_df = pl.concat(
            [used_df, data_df.select(used_df.columns)],
            how= 'vertical')
        prev_used_files_set = set(used_df['file'])
    else:
        prev_used_files_set = set()
    
    # new files can update and replace prev files for same year_qtr
    # prev_used_files has only one file per quarter
    # stack used_df and data_df, using the col names of used_df
    # find the latest new file for each quarter
    #   agg(pl.all().sort_by('date').last() -- ascending sort_by()
    # in each group, using all cols, sort_by date, select the last row
    used_files_set = set(data_df['file'])
    data_df = data_df.group_by(param.Update_param().YR_QTR_NAME)\
                     .agg(pl.all().sort_by('date').last())\
                     .sort(by= param.Update_param().YR_QTR_NAME)
        
    # and files to be used from the filtered input files
    files_to_read_set = set(data_df['file']) - prev_used_files_set
                       
# UPDATE record_dict
    used_files_list = sorted(list(used_files_set), reverse= True)
    record_dict['latest_file'] = used_files_list[0]
    '''
    used_files_dates_list = \
        pl.Series(data_df.select(pl.col('file').map_batches(
                                        hp.file_to_date_str, 
                                        return_dtype= pl.String)
                                 )
                  ).to_list()
    '''
    record_dict['prev_used_files'] = used_files_list
        
    record_dict['sources']['s&p'] = \
        env.SP_SOURCE
    record_dict['sources']['tips'] = \
        env.REAL_RATE_SOURCE
    
    return [record_dict, new_files_set, files_to_read_set]
    
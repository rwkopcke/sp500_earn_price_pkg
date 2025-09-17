import polars as pl
import json

from sp500_earn_price_pkg.helper_func_module \
    import helper_func as hp
    
import sp500_earn_price_pkg.config.config_paths as config
import sp500_earn_price_pkg.config.set_params as param
# param.Update_param()


def update():
    '''
        Inputs are Path() instances
        Returns 1 dict and 3 sets
            record_dict
            new_files_set
            files_to_read_set
            files_to remove_set
        
        Reads record_dict.pkl, writes to backup_record_dict.pkl
        Returns set of names for all new input files
        Returns set of names of input files to read (pertinent data)
        Returns set of names of prev used input files 
            that now contain stale data & to be delisted
    '''
    
    return_empty_objects = [dict(), set(), set()]
    
    # READ: load names of new data files, return if none
    input_sp_files_set = \
        set(str(f.name) 
            for f in config.Fixed_locations().INPUT_DIR.glob('sp-500-eps*.xlsx'))
    
    # if no input files, return up chain to main    
    if not input_sp_files_set:
        print('\n============================================')
        print(f'{config.Fixed_locations()().INPUT_DIR}')
        print('.  contains no sp input files')
        print(f'Return to menu of actions')
        print('============================================\n')
        return return_empty_objects
    
    if not config.Fixed_locations().INPUT_RR_ADDR.exists():
        print('\n============================================')
        print(f'{config.Fixed_locations()().INPUT_RR_ADDR} does not exist')
        print(f'Return to menu of actions')
        print('============================================\n')
        return return_empty_objects
    
    # READ: record_dict
    if config.Fixed_locations().RECORD_DICT_ADDR.exists():
        with open(config.Fixed_locations().RECORD_DICT_ADDR,'r') as f:
            record_dict = json.load(f)
            
    # use blank dict if necessary
    if (not config.Fixed_locations().RECORD_DICT_ADDR.exists()):
        print('\n============================================')
        print(f'No record_dict.json exists at\n{config.Fixed_locations().RECORD_DICT_ADDR}')
        print(f'Initializing record_dict')
        print('============================================\n')
         
        record_dict = {'sources': {'s&p': '',
                                   'tips': ''},
                       'latest_used_file': "",
                       'prev_used_files': [],
                       'prev_files': []}
        
    else:
        # WRITE: if prev record_dict exists, write backup
        with open(config.Fixed_locations().BACKUP_RECORD_DICT_ADDR, 'w') as f:
            json.dump(record_dict, f, indent= 4)
        print('\n============================================')
        print(f'Read record_dict from: \n{config.Fixed_locations().RECORD_DICT_ADDR}')
        print(f'Wrote record_dict to: \n{config.Fixed_locations().BACKUP_RECORD_DICT_ADDR}')
        print('============================================\n')
        
# create set of new files that were not previously seen
    prev_files_set = set(record_dict['prev_files'])
    new_files_set = input_sp_files_set - prev_files_set
    record_dict['prev_files'] = \
        sorted(list(prev_files_set | new_files_set), reverse= True)
    
# if nothing new, return up the chain to main
    if not new_files_set:
        print('\n============================================')
        print(f'No previously unseen files at')
        print(f'{config.Fixed_locations().RECORD_DICT_ADDR}')
        print('No data files have been written')
        print('============================================\n')
        return return_empty_objects

# ===========================================================
# NB from here, new_files_set is not empty
#    values for record_dict's keys may be empty
# ===========================================================
   
    # create df with the names and dates of the new_files
    data_df = pl.DataFrame(list(new_files_set), 
                           schema= ["file"],
                           orient= 'row')\
                .with_columns(pl.col('file')
                            .map_batches(hp.string_to_date,
                                         return_dtype= pl.Date)
                            .alias('date'))\
                .with_columns(pl.col('date')
                            .map_batches(hp.date_to_year_qtr)
                            .alias(param.Update_param().YR_QTR_NAME))
    
    # fetch used files list from record_dict
    # prev_used_files_list contains dicts, each with keys
    # 'file', 'date', param.Update_param().YR_QTR_NAME
    # one dict for each prev_used_file
    prev_used_files_list = record_dict['prev_used_files']
    if not prev_used_files_list:
        used_df = data_df
        prev_used_files_set = set()
        
    else:    
        # col names are the same as the keys for the dicts
        # contained in prev_used_files_list
        used_df = pl.DataFrame(prev_used_files_list)  
        prev_used_files_set = set(
            pl.Series(used_df['file']).to_list())
    
    # new files can update and replace prev files for same year_qtr
    # (prev_used_files has only one file per quarter)
    # find the latest new file for each quarter (agg(all.sort_by.last))
    # in each group, using all cols, sort_by date, select the last row
    # in new_files_list
    
        used_df = pl.concat([used_df.cast({'date': pl.Date}), 
                            data_df.select(used_df.columns)],
                            how= 'vertical')\
                    .group_by(param.Update_param().YR_QTR_NAME)\
                    .agg(pl.all().sort_by('date').last())\
                    .sort(by= param.Update_param().YR_QTR_NAME)

    # and files to be used from the new input files
    used_files_set = set(pl.Series(used_df['file']).to_list())
    files_to_read_set = used_files_set - prev_used_files_set

    # update prev_used_files for the new files
    # update_df['update_used_files']: list of dicts
    # cast allows json for saving record_dict
    update_df = used_df.cast({'date': pl.String})\
                       .select(pl.struct(pl.all())
                                 .alias('update_used_files'))
    
    record_dict['prev_used_files'] = sorted(
        update_df['update_used_files'],
        reverse= True,
        key= lambda x: x['date'])
    
    record_dict['latest_used_file'] = \
        record_dict['prev_used_files'][0]['file']
    
    record_dict['sources']['s&p'] = config.Fixed_locations().SP_SOURCE
    record_dict['sources']['tips'] = config.Fixed_locations().REAL_RATE_SOURCE
    
    return [record_dict, new_files_set, files_to_read_set]
    
from pathlib import Path

import polars as pl
import json

from sp500_earn_price_pkg.helper_func_module \
    import helper_func as hp
    
import config.config_paths as config
import config.set_params as params

env = config.Fixed_locations()
param = params.Update_param()

date = param.DATE_NAME
yr_qtr = param.YR_QTR_NAME


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
            
# WRITE: if prev record_dict exists, write temp backup
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


def record_dict(record_dict, input_files_set):
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
    def at_least_1_qtr_before_update_set(date_1, date_2):
        '''
            True if date_1 is at least 3 months before date_2
        '''
        if date_1 >= date_2:
            return False
        if date_1[0:4] < date_2[0:4]:
            return True
        if  3 <= (int(date_2[6:8]) - int(date_1[6:8])):
            return True
        return False
# CREATE sets of file names
# below, partition new_files_set: files to read & to archive
    prev_used_files_set = set(record_dict['prev_used_files'])
    prev_files_set = (prev_used_files_set |
                      set(record_dict['other_prev_files']))
    new_files_set = input_files_set - prev_files_set
    all_files_set = prev_files_set | new_files_set
    
# if nothing new, return
    if not new_files_set:
        return [dict(), set(), set()]
   
# UPDATE record_dict['prev_used_files'] for the new_used_files
    # create df with names of new & previously used files
    # create date and yr_qtr cols
    
    # previously used files that will remain "used"
    # their dates are 1yr < than that of any new file
    stable_used_files_set = \
        {file for file in prev_used_files_set
         if at_least_1_qtr_before_update_set(
             file[7:11], min(new_files_set)[7:11])}
    
    # files to be sorted into "used" and "other"
    mod_used_files_list = list(
        new_files_set | 
        (prev_used_files_set - stable_used_files_set)
    )
    
# patition mod_used_files into "used" and "other"
    data_df = pl.DataFrame(mod_used_files_list,
                            schema= ['file'],
                            orient= 'row')\
                .with_columns(pl.col.file
                    .map_batches(hp.file_to_date_str,
                                    return_dtype= pl.String)
                    .alias(date))\
                .with_columns(pl.col.date
                    .map_batches(hp.date_to_year_qtr,
                                    return_dtype= pl.String)
                    .alias(yr_qtr))\
                .group_by(yr_qtr)\
                .agg(pl.all().sort_by(date).last())\
                .sort(by= yr_qtr)
    # new files can update & replace prev files for same year_qtr
    # prev_used_files has only one file per quarter
    # find the latest file for each quarter
    #   agg(pl.all().sort_by('date').last() -- ascending sort_by()
    # in each group, sort_by date, select the last row
        
    # new "used" from the filtered "mod" list ...
    new_used_files_set = set(data_df['file'])
    used_files_set = \
        new_used_files_set | stable_used_files_set
    # all other files are "other"
    other_files_set = (
        set(record_dict['other_prev_files']) |
        (set(mod_used_files_list) - new_used_files_set))
    
    files_to_read_set = \
        new_files_set & new_used_files_set
                       
# UPDATE record_dict
    used_files_list = sorted(list(used_files_set),
                             reverse= True)
    record_dict['prev_used_files'] = used_files_list
    record_dict['latest_file'] = used_files_list[0]
    record_dict['other_prev_files'] = sorted(
        list(all_files_set - used_files_set),
        reverse= True
    )
    
    return [record_dict, new_files_set, files_to_read_set]
    
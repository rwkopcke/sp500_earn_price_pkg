import polars as pl
import json

from sp500_earn_price_pkg.helper_func_module \
    import helper_func as hp
    
import sp500_earn_price_pkg.config.config_paths as config
import sp500_earn_price_pkg.config.set_params as param

fixed_env = config.Fixed_locations()


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
    if fixed_env.RECORD_DICT_ADDR.exists():
        with open(fixed_env.RECORD_DICT_ADDR,'r') as f:
            record_dict = json.load(f)
            
# WRITE: if prev record_dict exists, write backup
        with open(fixed_env.BACKUP_RECORD_DICT_ADDR, 'w') as f:
            json.dump(record_dict, f, indent= 4)
        hp.message([
            f'Read record_dict from: \n{fixed_env.RECORD_DICT_ADDR}',
            f'Wrote record_dict to: \n{fixed_env.BACKUP_RECORD_DICT_ADDR}'
        ])
        
    else:
        # create record_dict from keys and empty values
        record_dict = fixed_env.RECORD_DICT_TEMPLATE
        hp.message([
            f'No record_dict.json exists at',
            f'{fixed_env.RECORD_DICT_ADDR}',
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
        fixed_env.ARCHIVE_DIR.glob(
                  fixed_env.INPUT_SP_FILE_GLOB_STR)
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
                .with_columns(pl.col('file')
                        .map_batches(hp.file_to_date,
                                     return_dtype= pl.Date)
                        .alias('date'))\
                .with_columns(pl.col('date')
                        .map_batches(hp.date_to_year_qtr,
                                     return_dtype= pl.String)
                        .alias(param.Update_param().YR_QTR_NAME))
    
    # fetch prev_used_files list from record_dict
    # prev_used_files contains a list of dicts, each with keys
    # 'file', 'date', param.Update_param().YR_QTR_NAME
    # one dict for each prev_used_file
    prev_used_files_list = list(record_dict['prev_used_files']['file'])
    if not prev_used_files_list:
        used_df = data_df
        prev_used_files_set = set()
        
    else:
        # col names are the same as the keys 
        # contained in prev_used_files_list
        used_df = pl.DataFrame(prev_used_files_list)\
                    .cast({'date': pl.Date})
        prev_used_files_set = set(
            pl.Series(used_df['file']).to_list())
    
    # new files can update and replace prev files for same year_qtr
    # prev_used_files has only one file per quarter
    # stack used_df and data_df, using the col names of used_df
    # find the latest new file for each quarter
    #   agg(pl.all().sort_by('date').last() -- ascending sort_by()
    # in each group, using all cols, sort_by date, select the last row
        data_df = pl.concat([used_df, 
                            data_df.select(used_df.columns)],
                            how= 'vertical')\
                    .group_by(param.Update_param().YR_QTR_NAME)\
                    .agg(pl.all().sort_by('date').last())\
                    .sort(by= param.Update_param().YR_QTR_NAME)
        
    # and files to be used from the new input files
    files_to_read_set = \
        set(pl.Series(data_df['file']).to_list()) - prev_used_files_set

    # update prev_used_files for the new files
    # update_df['update_used_files']: list of dicts
    # cast allows json to save record_dict as json
    update_df = data_df.cast({'date': pl.String})\
                       .select(pl.struct(pl.all())
                            .alias('updated_used_files'))
                       
# UPDATE record_dict
    record_dict['prev_used_files'] = sorted(
        update_df['updated_used_files'],
        reverse= True,
        key= lambda x: x['date'])
    
    record_dict['latest_file'] = \
        record_dict['prev_used_files'][0]
    
    record_dict['quarters_in_record'] = sorted(
        data_df[param.Update_param().YR_QTR_NAME],
        reverse= True)
        
    record_dict['sources']['s&p'] = \
        fixed_env.SP_SOURCE
    record_dict['sources']['tips'] = \
        fixed_env.REAL_RATE_SOURCE
    
    return [record_dict, new_files_set, files_to_read_set]
    
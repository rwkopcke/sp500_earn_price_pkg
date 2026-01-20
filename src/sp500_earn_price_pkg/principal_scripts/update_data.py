'''
    This program reads selected data from S&P
    and from the 10-year TIPS rate from FRED
    It creates polars dataframes, which it writes to .parquet files
    It creates a record of its transactions as a dict, 
        which it writes to a .json file
        
    see config_paths.py
        for the addresses of the files within this project are declared
        for advice in resetting paths for debugging and for reinitializing
        the project's output files

    The polars dataframes in input_output/output contain
        historical data, 
        projections of earnings, and
        earnings and price data by industry 
    See the README file for this project
'''
import polars as pl
import polars.selectors as cs

from sp500_earn_price_pkg.principal_scripts.code_segments.update_data \
    import update_record

from sp500_earn_price_pkg.helper_func_module import helper_func as hp
from sp500_earn_price_pkg.principal_scripts.code_segments.update_data \
    import read_data as read
# contains all scripts that write to files
from sp500_earn_price_pkg.principal_scripts.code_segments.update_data \
    import write_data_to_files as write

import config.config_paths as config
import config.set_params as params

env = config.Fixed_locations
param = params.Update_param

date = param.DATE_NAME
yr_qtr = param.YR_QTR_NAME
year = param.ANNUAL_DATE


def update():
    ''' Check Input files from S&P and Fred for new data
        Insert any new data into existing history
        Write the new data, as appropriate, to
            sp500_pe_df_actuals.parquet
            sp500_pe_df_estimates.parquet
            sp500_ind_df.parquet
        Record these transactions in
            record_dict.json
    '''
    
## ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
## +++++  ensure sp input files exist and are named consistently  +++++++++
## +++++   format of std name: sp-500-eps DATE_FMT_SP_FILE    +++++++++++++
## ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    sp_input_files_set = read.verify_valid_input_files()
    
    if not sp_input_files_set:
        hp.message([
            'no valid input files, see messages above'
            ])
        return  #back to entry.py
    else:
        hp.message([
            f'New input files verified: \n{sp_input_files_set}'
        ])
    
    sp_input_files_set = \
        read.ensure_consistent_file_names(sp_input_files_set)
        
    hp.message([
            f'New files for processing: \n{sp_input_files_set}'
        ])
        
## +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++              
## +++++  update record_dict for new files  ++++++++++++++++++++++++++++++++
## +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    # fetch record_dict - if record_dist is None, create it
    record_dict = update_record.fetch()
         
    # update record_dict for new_files_set and files_to_read_set
    [record_dict, new_files_set, files_to_read_set] = \
         update_record.record_dict(record_dict, sp_input_files_set)
    
    if not files_to_read_set:
        hp.message([
            'No new input files',
            'Stop Update and return to menu of actions'
        ])
        '''
        # the parquet and json files have not been altered at this point
        write.restore_data_stop_update(location=
            "update_data.py() update record dict for new files",
            exit= False)
        '''
        write.purge_files(env.INPUT_DIR, 
                          env.INPUT_SP_FILE_GLOB_STR)
        return  #back to entry.py

## +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++              
## +++++  update historical aggregate data  +++++++++++++++++++++++++++++++++
## +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ 
    latest_file = record_dict['latest_file']
    
    hp.message([
        f'Update historical data from: {latest_file}',
        f'in directory: \n{env.INPUT_DIR}'
    ])
    
## EXISTING HISTORICAL DATA from .parquet file (not yet updated)
    # the rows (qtrs) to be updated are the rows that
    # contain null in the op_eps col => assumes history is not revised
    # yrqtr_no_update_set: rows NOT to be updated
    
    actual_df = read.history_data()
    
    # find the set of rows to update in actual_df
    if not actual_df.is_empty():
        [dates_to_update_set,
         min_date_to_update,
         min_yr_qtr_to_update] = \
            read.find_qtrs_without_op_earn(actual_df)
    else:
        dates_to_update_set = set()
        min_date_to_update = None
        min_yr_qtr_to_update = None
    
## NEW HISTORICAL DATA for SP500 from latest sp file
    # activate latest xlsx wkbk and sheet with data for sp
    [add_df, cell_list] = read.history_loader(
                                    latest_file,
                                    min_date_to_update)
    
## MARGINS add to add_df
    add_df = add_df.join(read.margin_loader(
                                  latest_file,
                                  min_date_to_update,
                                  cell_list), 
                         how="left", 
                         on= param.YR_QTR_NAME,
                         coalesce= True)

## QUARTERLY DATA for SP500 add to add_df
    # ensure all dtypes (if not string or date-like) are float32
    # some dtype are null when all col entries in short df are null
    add_df = add_df.join(read.qtrly_loader(latest_file,
                                           min_yr_qtr_to_update),  
                    how= "left", 
                    on= [param.YR_QTR_NAME],
                    coalesce= True)
    
# REAL INTEREST RATES, eoq, from FRED DFII10
    add_df = add_df.join(read.fred_reader(
                                env.INPUT_RR_FILE,
                                min_yr_qtr_to_update),
                         how= 'left',
                         on= param.YR_QTR_NAME,
                         coalesce= True)
    
## ACTUAL_DF update: remove rows to be updated and concat with add_df
    # align cols of actual_df with add_df (casting null cols--
    #    those that are not pl.String or pl.Float32-- to pl.Float32)
    #.   some cols in add_df can be entirely null
    # ensure rows do not overlap
    if actual_df.is_empty():
        actual_df = add_df.sort(by= date)
    else:
        actual_df = \
            pl.concat([add_df.cast({
                            ~cs.by_dtype(pl.Date, pl.String): 
                            pl.Float32}),
                        actual_df.select(add_df.columns)
                                 .filter(~pl.col(date).is_in(
                                     dates_to_update_set))],
                        how= 'vertical')\
              .sort(by= yr_qtr)

## +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
## +++++  fetch & update historical industry data  +++++++++++++++++++++++++++
## +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    # read stored data
    ind_df = read.industry_data()
    
    if not ind_df.is_empty():
        years_no_update_set = \
            read.find_yrs_without_rep_earn(ind_df)
    else:
        years_no_update_set = set()
        
    # find new industry data
    add_ind_df = read.industry_loader(latest_file,
                                      years_no_update_set)
    
    # add col with Q4 value of real_int_rate each year from actual_df
    add_ind_df = add_ind_df.join(
                    actual_df.select([yr_qtr, param.RR_NAME])
                             .filter(pl.col(yr_qtr)
                                       .map_elements(lambda x: x[-1:]=='4',
                                            return_dtype= pl.Boolean))
                             .with_columns(pl.col(yr_qtr)
                                       .map_elements(lambda x: x[0:4],
                                            return_dtype= pl.String)
                                       .alias(year))
                             .cast({year: param.YEAR_ENUM})
                             .drop(yr_qtr),
                    on= year,
                    how= 'left',
                    coalesce= True)\
            .sort(by= year, descending= True)\
            .cast({cs.float() : pl.Float32})
    
    if ind_df.is_empty():
        ind_df = add_ind_df.sort(by= year, 
                                 descending= True)
    else:
        years = pl.Series(add_ind_df[year]).to_list()
        
        ind_df = pl.concat([add_ind_df,
                            ind_df.filter(
                                ~pl.col(year)
                                   .is_in(years))],
                            how= 'vertical')
    
## +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++              
## +++++++++++++++++ fetch projections  ++++++++++++++++++++++++++++++++++++
## ++++++++++++ proj_dict: {yr_qtr: df of proj}  +++++++++++++++++++++++++++
## +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    # fetch history
    proj_dict = read.projection_data()

    # Fetch files_to_read from inputs
    # Update proj_dict with info in files_to_read
    # use proj of earnings for latest input file in each yr_qtr
    # ordinarily a very short set
    failure_to_read_lst = []
    for file in files_to_read_set:
        [proj_date_df, year_quarter] = read.proj_loader(file)

        # if any date is None, abort file and continue
        if (None in proj_date_df[param.DATE_NAME]):
            hp.message([
                'In main(), projections:',
                f'Skipped sp-500 {file} missing projection date'
            ])
            failure_to_read_lst.append(file)
            continue
        
        # accumulate proj_date_dfs in proj_dict, 
        # key for each proj_date_df is its year_quarter
        proj_dict[year_quarter] = proj_date_df
        
    # housekeeping summary for files_to_read
    l = len(files_to_read_set)
    n = len(failure_to_read_lst)
    hp.message([
        'Reading input projection files is complete',
        f'\t{l - n} new input files read',
        f'\tfrom {config.Fixed_locations().INPUT_DIR}',
        f'\t{n} files not read:',
        f'\t{failure_to_read_lst}'
    ])

## +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
## +++++ write dfs to parquet, record to json, temp files to backups +++++++
## ++++++++++ archive s&pinput files  +++++++++++++++++++++++++++++++++++++
## +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

    write.history(actual_df)
    write.industry(ind_df)
    write.projection(proj_dict)
    write.record(record_dict)
    write.archive_sp_input_xlsx(new_files_set)
    # tidy the temp files and verify backup is empty
    write.write_temp_to_backup()
    return

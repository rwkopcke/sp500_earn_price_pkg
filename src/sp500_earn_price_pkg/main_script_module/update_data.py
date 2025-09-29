'''This program reads selected data from S&P, sp-500-eps-est.xlsx
        https://www.spglobal.com/spdji/en/search/?query=index+earnings&activeTab=all
   and from the 10-year TIPS rate from FRED: 
        https://fred.stlouisfed.org/series/DFII10
   It writes these data as polars dataframes to .parquet files
        and writes a record of the files that it has writen
        as a dictionary to a .json file
        
   see config_paths.py
        for the addresses of the files within this project are declared
        for advice in resetting paths for debugging and for reinitializing
        the project's output files

   The polars dataframes in input_output
        the latest projections of earnings for the
        S&P500 within each quarter since late 2017. 
   A separate polars dataframe contains
        the actual earnings and the value of the index for each quarter 
        beginning in 1988. This dataframe also contains actual values for 
        operating margins, revenues, book values, dividends, and other 
        actual data reported by S&P, plus actual values for the 10-year TIPS.
   
   The addresses of documents for this project appear in this program's 
   project directory: S&P500_PE/sp500_pe/config_paths.py
'''

#######################  Parameters  ###################################
import gc

import polars as pl
import polars.selectors as cs
from openpyxl import load_workbook

from sp500_earn_price_pkg.helper_func_module import (
    update_record,
    update_write_history_and_industry_files,
    update_proj_hist_files,
    update_write_proj_files,
    update_write_record,
)

from sp500_earn_price_pkg.helper_func_module \
    import helper_func as hp
from sp500_earn_price_pkg.helper_func_module \
    import read_data_func as rd

import sp500_earn_price_pkg.config.config_paths as config
import sp500_earn_price_pkg.config.set_params as params

import sp500_earn_price_pkg.config.set_params as params
param = params.Update_param()


#######################  MAIN Function  ###############################

def update():
    ''' Check Input files from S&P and Fred for new data
        Insert any new data into existing history
        Write the updates to
            sp500_pe_df_actuals.parquet
            sp500_pe_df_estimates.parquet
            sp500_ind_df.parquet
        Record these transactions in
            record_dict.json
            
        config.Fixed_locations(): paraneters from config_paths.py
    '''
    
## ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
## +++++  ensure sp input files exist and are named consistently  +++++++++
## +++++   format of std name: sp-500-eps DATE_FMT_SP_FILE    +++++++++++++
## ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

    sp_input_files_set = \
        rd.files_valid(config.Fixed_locations().INPUT_DIR,
                       config.Fixed_locations().INPUT_SP_FILE_GLOB_STR,
                       config.Fixed_locations().INPUT_RR_FILE)
    
    if not sp_input_files_set:
        hp.message([
            'no valid input files, see messages above'
            ])
        return
    
    sp_input_files_set = \
        rd.ensure_consistent_names(
            config.Fixed_locations().INPUT_DIR,
            sp_input_files_set)
    
## +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++              
## +++++  update record_dict for new files  ++++++++++++++++++++++++++++++++
## +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

# A. retrieve, uses, and save all dates as datetime.date() objects

# 1. load record dict <replace full history of files read with full history of yr_qtr>
# 2. check Input sp files for new data
# 3. load history, returning actual df [read_parquet]
# 4. set rows to update [new rows: set of all dates less set not to update]
# 5. add_df: e data in rows to update
# 6. load margins for rows to update [read_xl]
# 7. add to add_df
# 8. load qtrly data for rows to update [read_xl]
# 9. add to add_df
#10. load rr data for dates in rows to update [read_xl]
#11. add to add_df
#12. concate history rows not to update with add_df
#13. write new history to parquet [write_parquet]
#    update record dict
#       latest dt.date for xlsx used
#       latest yr_qtr for xlsx used
#       latest yr_qtr with actual op_eps data
#+++++++++++++++++++++++++++++++++++++++++++++++++++
#14. load historical ind data, returning actual df [read_parquet]
#15. set rows to update
#16. add_df: ind data for rows to update [read_xls]
#17. concat
#18. write [write_parquet]
#+++++++++++++++++++++++++++++++++++++++++++++++++++++
#19. create message function in hp.message(*args: list of strings

    # fetch record_dict - if record_dist is None, create it
    record_dict = update_record.fetch()
         
    # update record_dict for new_files_set and files_to_read_set
    [record_dict, new_files_set, files_to_read_set] = \
         update_record.update(record_dict, sp_input_files_set)
    
    if not files_to_read_set:
        hp.message([
            'No new input files',
            'Stop Update and return to menu of actions'
        ])
        return
    
    quit()

## +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++              
## +++++  fetch historical aggregate data  +++++++++++++++++++++++++++++++++
## +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ 
    
    hp.message([
        f'Update historical data from: {record_dict['latest_file']['file']}',
        f'in directory: \n{config.Fixed_locations().INPUT_DIR}'
    ])
    
    ## ACTUAL DATA from existing .parquet file (not yet updated with new data)
    # the rows (qtrs) to be updated are the rows that
    # contain null in the op_eps col => assumes history is not revised
    # rows_no_update_set: yr_qtr values for rows NOT to be updated
    if config.Fixed_locations().OUTPUT_HIST_ADDR.exists():
        actual_df = \
            pl.read_parquet(config.Fixed_locations().OUTPUT_HIST_ADDR)
        
        rows_not_to_update_set = \
            set(pl.Series(actual_df
                    .filter(pl.col('op_eps').is_not_null())
                    .select(pl.col(param.Update_param().YR_QTR_NAME)))
                  .to_list())
    else:
        rows_not_to_update_set = {}
    
## REAL INTEREST RATES, eoq, from FRED DFII10
    active_workbook = \
        load_workbook(filename= config.Fixed_locations().INPUT_RR_ADDR,
                      read_only= True,
                      data_only= True)
    active_sheet = active_workbook[param.Update_param().SHT_RR_NAME]
    real_rt_df = rd.fred_reader(active_sheet,
                                **param.Update_param().SHT_FRED_PARAMS)

## NEW HISTORICAL DATA
    ## WKSHT with new historical values for P and E from new excel file
    latest_file_addr = \
        config.Fixed_locations().INPUT_DIR / record_dict["latest_file"]['file']
    active_workbook = load_workbook(filename= latest_file_addr,
                                    read_only= True,
                                    data_only= True)
    # most recent date and prices
    active_sheet = active_workbook[param.Update_param().SHT_EST_NAME]

    # add_df, dates and latest prices, beyond existing historical data
    date_of_sheet = \
        rd.read_sheet_date(active_sheet, param.Update_param().DATE_KEYS,
                           "A", 1)
    
    
    add_df = \
        rd.read_sp_date(active_sheet, 
                        **param.Update_param().SHT_EST_DATE_PARAMS,
                        include_prices= True)
    
    # load new historical data
    # omit rows whose yr_qtr appears in the rows_no_update list
    df = rd.sp_loader(active_sheet,
                      rows_not_to_update_set,
                      **param.Update_param().SHT_HIST_PARAMS)
    
    # if any date is None, quit
    if (name_date is None or
        any([item is None
            for item in add_df['date']])):
        
        hp.message([
            f'{latest_file_addr} \nmissing history date',
            f'Name_date: {name_date}',
            actual_df['date']
        ])
        quit()
        
    # update add_df with new historical data
    add_df = pl.concat([add_df, df], how= "diagonal")
               
    # include rr in add_df
    add_df = add_df.join(real_rt_df, 
                         how="left", 
                         on=[param.Update_param().YR_QTR_NAME],
                         coalesce= True)
    
    del real_rt_df
    del df
    gc.collect()
        
## MARGINS add to add_df
    margins_df = rd.margin_loader(active_sheet,
                                  rows_not_to_update_set,
                                  **param.Update_param().SHT_BC_MARG_PARAMS)
    
    add_df = add_df.join(margins_df, 
                         how="left", 
                         on= param.Update_param().YR_QTR_NAME,
                         coalesce= True)
    
    del margins_df
    gc.collect()

## QUARTERLY DATA add to add_df
    active_sheet = active_workbook[param.Update_param().SHT_QTR_NAME]

    # ensure all dtypes (if not string or date-like) are float32
    # some dtype are null when all col entries in short df are null
    qtrly_df = rd.sp_loader(active_sheet,
                            rows_not_to_update_set,
                            **param.Update_param().SHT_QTR_PARAMS)\
                 .cast({~(cs.temporal() | cs.string()): pl.Float32,
                        cs.datetime(): pl.Date})
    
    add_df = add_df.join(qtrly_df,  
                         how= "left", 
                         on= [param.Update_param().YR_QTR_NAME],
                         coalesce= True)
    
    del qtrly_df
    gc.collect()
    
## ACTUAL_DF update: remove rows to be updated and concat with add_df
    # align cols of actual_df with add_df
    # ensure rows do not overlap
    
    if config.Fixed_locations().OUTPUT_HIST_ADDR.exists():
        actual_df = \
            pl.concat([add_df.filter(
                               ~pl.col(param.Update_param().YR_QTR_NAME)
                               .is_in(rows_not_to_update_set)),
                        actual_df.select(add_df.columns)
                                .filter(pl.col(param.Update_param().YR_QTR_NAME)
                                .is_in(rows_not_to_update_set))],
                        how= 'vertical')\
                      .sort(by= param.Update_param().YR_QTR_NAME)
    else:
        actual_df = add_df.sort(by= param.Update_param().YR_QTR_NAME)
    
    del add_df
    gc.collect()

## +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
## +++++  fetch historical industry data  ++++++++++++++++++++++++++++++++++++
## +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

    # read stored data
    if config.Fixed_locations().OUTPUT_IND_ADDR.exists():
        ind_df = pl.read_parquet(config.Fixed_locations().OUTPUT_IND_ADDR)\
                        .sort(by= 'year', descending= True)
                    
        years_no_update = set(pl.Series(ind_df
                                .drop_nulls(subset='SP500_rep_eps')
                                .select(pl.col('year')))
                                .to_list())
    else:
        years_no_update = []
    
    # find new industry data
    active_sheet = active_workbook[param.Update_param().SHT_IND_NAME]
    add_ind_df = rd.industry_loader(active_sheet,
                                    years_no_update,
                                    **param.Update_param().SHT_IND_PARAMS)
    # add col with Q4 value of real_int_rate each year from actual_df
    add_ind_df = add_ind_df.join(
                 actual_df.select([param.Update_param().YR_QTR_NAME, 'real_int_rate'])
                          .filter(pl.col(param.Update_param().YR_QTR_NAME)
                                    .map_elements(lambda x: x[-1:]=='4',
                                                return_dtype= bool))
                          .with_columns(pl.col(param.Update_param().YR_QTR_NAME)
                                    .map_elements(lambda x: x[0:4],
                                                return_dtype= str)
                                    .alias('year'))
                          .drop(param.Update_param().YR_QTR_NAME),
                 on= 'year',
                 how= 'left',
                 coalesce= True)\
            .sort(by= 'year', descending= True)\
            .cast({~cs.string() : pl.Float32})
    
    if config.Fixed_locations().OUTPUT_IND_ADDR.exists():
        years = pl.Series(add_ind_df['year']).to_list()
        ind_df = pl.concat([add_ind_df,
                            ind_df.filter(~pl.col('year')
                                            .is_in(years))],
                            how= 'vertical')
    else:
        ind_df = add_ind_df.sort(by= 'year', descending= True)
        
## +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
## +++++ write history, industry files  ++++++++++++++++++++++++++++++++++++
## +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

    actual_df = actual_df.cast({cs.float(): pl.Float32,
                                cs.integer(): pl.Int16})
    ind_df = ind_df.cast({cs.float(): pl.Float32,
                          cs.integer(): pl.Int16})

    update_write_history_and_industry_files.write(actual_df, ind_df)
    
    del actual_df
    del ind_df
    gc.collect()
    
## +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++              
## +++++ fetch projections & archive all proj input files  +++++++++++++++++
## +++ proj_dict: yr_qtr keys & df of proj as values +++++++++++++++++++++++
## +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

    # fetch history
    proj_dict = update_proj_hist_files.update()
    
    # Fetch files_to_read from inputs
    # Update proj_dict with info in files_to_read
    # use proj of earnings for latest input file in each yr_qtr
    # ordinarily a very short set
    failure_to_read_lst = []
    for file in files_to_read_set:
        # echo file name and address to console
        active_workbook = \
            load_workbook(filename=  config.Fixed_locations().INPUT_DIR / file,
                          read_only= True,
                          data_only= True)
        active_sheet = active_workbook[param.Update_param().SHT_EST_NAME]
        print(f'\n input file: {file}')    
        
        # projections of earnings
        # read date of projection, no prices or other data
        name_date, _ = \
            rd.read_sp_date(active_sheet,
                            **param.Update_param().SHT_EST_PROJ_DATE_PARAMS)
        name_date = name_date.date()
        year_quarter = hp.date_to_year_qtr([name_date])[0]
    
        # load projections for the name_date
        proj_date_df = rd.sp_loader(
            active_sheet,[],**param.Update_param().SHT_EST_PROJ_PARAMS)

        # if any date is None, abort file and continue
        if (name_date is None or
            None in proj_date_df['date']):
            hp.message([
                'In main(), projections:',
                f'Skipped sp-500 {name_date} missing projection date'
            ])
            failure_to_read_lst.append(file)
            continue
        
        # accumulate proj_date_dfs in proj_dict, 
        # key for each proj_date_df is its year_quarter
        
        proj_dict[year_quarter] = proj_date_df.cast(
                                    {cs.float(): pl.Float32,
                                     cs.integer(): pl.Int16})
        
    # Print housekeeping summary for files_to_read
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
## +++ write updated proj_dict to parquet file +++++++++++++++++++++++++++++
## +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

    update_write_proj_files.write(proj_dict, new_files_set)
        
## +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
## +++++ write record ++++++++++++++++++++++++++++++++++++++++++++++++++++++
## +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

    update_write_record.write(record_dict)
    
    return
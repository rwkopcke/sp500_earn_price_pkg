'''
   these are functions used by the update_data and display_data
   scripts
        
   access these values in other modules by
        import sp500_pe.read_data_func as rd
'''
import datetime

from openpyxl import load_workbook
import openpyxl.utils.cell as ut_cell
from openpyxl.utils import coordinate_to_tuple
import polars as pl
import polars.selectors as cs

from sp500_earn_price_pkg.helper_func_module \
    import helper_func as hp
    
import sp500_earn_price_pkg.config.config_paths as config
import sp500_earn_price_pkg.config.set_params as params

env = config.Fixed_locations()
rd_param = params.Update_param()
    
    
def verify_valid_input_files():
    '''
        Verifies that input_dir exists
        Verifies that input_dir contains
            at least one sp input file
            only one TIPS input file
        If true, returns set of sp files 
        if false, returns empty set
    '''
    
    input_dir = env.INPUT_DIR
    sp_glob_str = env.INPUT_SP_FILE_GLOB_STR
    rr_name = env.INPUT_RR_FILE
    
    if not input_dir.exists():
        hp.message([
            f'{input_dir} does not exist'
        ])
        return {}

# both sp and rr files present?
# READ: data files in input_dir
    input_sp_files_set = \
        set(str(f.name) for f in 
            input_dir.glob(sp_glob_str))
    rr_file_lst = [str(f.name) for f in 
                   input_dir.glob(rr_name)]
    
    # if no sp input files
    if not input_sp_files_set:
        hp.message([
            f'no sp input files in \n{input_dir}',
            f'sp file names must conform to {sp_glob_str}'
        ])
        
    # if no unique rr input file
    if not (len(rr_file_lst) == 1):
        hp.message([
            f'no unique {rr_name} in \n{input_dir}',
            f'require one TIPS file with name {rr_name}'
        ])
        # set signal: inputs not valid
        input_sp_files_set = {}
    
    return input_sp_files_set


def ensure_consistent_file_names(names_set):
    '''
    Finds date in each sp input xlsx
        Amends name of input file to:
            "sp-500-eps yyyy-mm-dd.xlsx"
        where the date is taken from the
            "Estimates" sheet, near the top of col A
        amends the names of sp files in input_dir
            to conform to the format above
            
        quit() if cannot find a date in the file
        
        Returns the set of std names
        
        # https://www.programiz.com/python-programming/datetime/strftime
        # https://duckduckgo.com/?q=python+string+to+datetime&t=osx&ia=web
    '''
    
    input_dir = env.INPUT_DIR
    
    output_sp_files_set = set()
    for file in names_set:
        path_name = input_dir / file
        sheet = find_wk_sheet(
            path_name,
            rd_param.SHT_EST_NAME)
        
        row_idx = 1
        # find file's date for file's name
        while row_idx < 11:
            val = sheet.cell(row= row_idx, column= 1).value
            
            # returns "" if no date as str or datetime obj
            date_str = hp.cast_date_to_str(val)
                
            if date_str:
                new_name_file = f'sp-500-eps {date_str}.xlsx'
                # add name to return set
                output_sp_files_set.add(new_name_file)
                # rename file
                path_name.rename(input_dir / new_name_file)
                
                break
                
            row_idx += 1
            
        if row_idx == 11:
            hp.message([
                'ERROR read_data_func.std_names for files',
                'found no date in first 10 rows of',
                f'{input_dir / file}',
                'inspect file for date'
                ])
            quit()
        
    return output_sp_files_set


def find_wk_sheet(path_name, sheet_name):
    '''
        Receive 
            path_name, address of xlsx
            sheet_name, name of sheet in xlsx
        Return sheet (openpyxl) for reading
    '''
    workbook = load_workbook(
            filename= path_name,
            read_only= True, 
            data_only= True)
    return workbook[sheet_name]


def read_existing_history():
    '''
        Read saved history from parquet file, and
        Return as pl.df 
    '''
    if env.OUTPUT_HIST_ADDR.exists():
        return pl.read_parquet(env.OUTPUT_HIST_ADDR)
    else:
        return pl.DataFrame()

    
def update_history(file, omit_yrqtr_set):
    '''
        Receives an xlsx sheet
        Reads two types of data from sheet
            rectangular block of reported earn
            actual qtr-end prices since last
                qtr that contains reported earn
        Returns a pl.dataframe
            new historical data from the sheet
        sheet is a list of lists,
            a matrix extracted from the sheet
    '''
    sheet = find_wk_sheet(
            env.INPUT_DIR / file,
            rd_param.SHT_EST_NAME)
    
# 1st: read rectangular block of reported earn
    # find start_row = row for key + 1
    #      stop_row = row for key - 1
    start_row = 1 + \
        find_key_in_xlsx(sheet, 
                         row_num= 1, col_ltr= 'A', 
                         keys= rd_param.HISTORY_KEYS)
    if omit_yrqtr_set:
        keys= omit_yrqtr_set
    else:
        keys= None
    stop_row = -1 + find_key_in_xlsx(sheet, 
                         row_num= start_row, col_ltr= 'A',
                         keys= keys)
    
    data = data_block_reader(sheet, 
                             start_row, stop_row,
                             ** rd_param.SHT_HIST_PARAMS)
    
    df = pl.DataFrame(data,
                      schema= rd_param.COLUMN_NAMES,
                      orientation= "row")\
           .cast({cs.float(): pl.Float32,
                  cs.datetime(): pl.Date})\
           .with_columns(pl.col('date')
                            .map_batches(hp.date_to_year_qtr)
                            .alias(rd_param.YR_QTR_NAME))\
           .filter(~pl.col(rd_param.YR_QTR_NAME)
                   .is_in(omit_yrqtr_set))
    
    
    
    return read_sheet_data(
                sheet, 
                **rd_param.SHT_HIST_PARAMS)
    
    
'''
SHT_HIST_PARAMS = {
        'start_row_key': ACTUAL_KEYS,
        'stop_row_key': None,
        'first_col_key': None,
        'last_col_key': None,
        'start_row': None,
        'stop_row': None,
        'first_col': 'A',
        'last_col': 'J',
        'skip_cols': [4, 7]
    }
'''


def read_sheet_data(wksht, 
                    date_keys, 
                    date_search_col, 
                    date_start_row):
    '''
        fetch date from s&p's excel workbook.
    '''
    
    if date_keys is None:
        hp.message([
            f'Date keys are {date_keys} for {wksht}',
            'read_data_func.py: read_sheet_date'
        ])
        quit()
    
    # find date of wkbk: date cell in row below date_keys cell
    date_row = find_key_in_xlsx(wksht, date_start_row, date_search_col, 
                               keys= date_keys) + 1

    if (date_row == 1):
        hp.message([
            'In read_data_funct.py read_sheet_date, date keys:',
            f'Found no {date_keys} in {wksht}'
        ])
        quit()
        
    sheet_date = hp.dt_str_to_date(
            wksht[f'{date_search_col}{date_row}'].value)
    
    if not isinstance(sheet_date, datetime.date):
        hp.message([
            'In read_data_funct.py read_sheet_date:',
            f'{sheet_date} is not a datetime.date object'
        ])
        quit()
    
    return sheet_date

    
def data_block_reader_sig(wksht, 
                      start_row, stop_row,
                      first_col, last_col, 
                      skip_cols= [], skip_rows= []):
    '''
    shows signature from below
    '''
    pass

    
def find_key_in_xlsx(wksht, row_num, col_ltr, 
                     keys= None,
                     is_row_iter= True):
    '''
        for keys, which is either None or a list of strings,
        find the cell whose value is in keys.
        start at cell specified by col_ltr, row_num.
        if is_row_iter= True, iter "down" rows
        if False, iter across cols
        
        when a match occurs, return row_num, col_num, col_ltr
        when no match occurs, return msg
    '''
    
    if (keys is not None):
        # ensure keys is a list
        if (type(keys) is not list):
            keys = list(keys)
        
        # keys must be a (nonempty) list of str
        if ((not all([type(item) is str 
                      for item in keys])) 
            or
            (not keys)):
            hp.message([
                'ERROR keys is not a list of str',
                'In helper_func.py, find_key_in_xlsx(),',
                f'keys: {keys}',
                f'is_row_iter: {is_row_iter}',
                f'row_start: {row_num}; col_start: {col_ltr}'
            ])
            quit()
    
    # initial values for iter loop
    _, col_num = coordinate_to_tuple(f'{col_ltr}{row_num}')
    if is_row_iter:
        max_to_read = wksht.max_row
        iter_idx = row_num
    else:
        max_to_read = wksht.max_column
        iter_idx = col_num
    max_to_read += 1
    
    # iter loop
    while iter_idx < max_to_read:
        val = wksht.cell(row= row_num, column= col_num).value
                
        if (keys is not None):
            if (val in keys):
                return [row_num, col_num, \
                    ut_cell.get_column_letter(col_num)]
        else:
            if (val is None):
                return row_num, col_num, \
                    ut_cell.get_column_letter(col_num)
        
        if is_row_iter:
            row_num += 1
        else:
            col_num += 1
        iter_idx += 1
        
    hp.message([
        'ERROR search found no value that matchs keys',
        'In read_data_func.py, find_key_in_xlsx,',
        f'keys: {keys}',
        f'is_row_iter: {is_row_iter}'
    ])
    quit()
                                                            
    return 0
            
    
'''
SHT_EST_DATE_PARAMS = {
        'date_keys' : ['Date', 'Data as of the close of:'],
        'date_search_col': 'A',
        'date_start_row': 1,
        'value_col_1' : 'D',
        'date_key_2' : ACTUAL_KEYS,
        'value_col_2' : 'B',
        'column_names' : COLUMN_NAMES,
        'yr_qtr_name' : YR_QTR_NAME
'''
def read_sp_date(wksht,
                 date_keys, date_search_col,
                 date_start_row,
                 value_col_1, 
                 date_key_2, value_col_2,
                 column_names, yr_qtr_name,
                 include_prices= False):
    '''
        fetch date from s&p's excel workbook.
        fetch recent s&p prices, which occur after
            the last reported set of earnings if
            include_prices= True.
        the historical record of prices is current
        the record for earnings appears with a lag
            prices are quarter-end
            earnings are reported quarterly
            
        return the workbook's date in name_date,
            type datetime.date
        if include_prices= True,
            also return df with recent prices
            and their dates, type datetime.date
    '''
    date_lst = []
    price_lst = []
    
    #date_lst.append(name_date)
    price_lst.append(wksht[f'{value_col_1}{key_row + 1}'].value)
    
    # fetch next date and price
    key_row = find_key_in_xlsx(wksht, key_row, 'A', 
                               key_values= date_key_2)
    
    if (key_row == 0):
        hp.message([
            'In read_data_func.py read_sp_date, for date_key_2:',
            f'Found no {date_key_2} in {wksht}'
        ])
        quit()
    
    date_lst.append(hp.dt_str_to_date(
        wksht[f'A{key_row - 2}'].value))
    price_lst.append(wksht[f'{value_col_2}{key_row -2}'].value)
    
    df = pl.DataFrame({
                column_names[0]: date_lst,
                column_names[1]: price_lst},
                schema= {column_names[0]: pl.Date, 
                            column_names[1]: pl.Float32})\
           .with_columns(pl.col('date')
                            .map_batches(hp.date_to_year_qtr)
                            .alias(yr_qtr_name))
    
    return #[name_date, df]


def data_block_reader(wksht, 
                      start_row, stop_row,
                      first_col, last_col, 
                      skip_cols= [], skip_rows= []):
    """
        Returns a rectangular block of data from an xlsx 
            as a list of lists
        From start_row to stop_row, start_col to stop_col
            inclusive
            skip_cols: a list of numbers
            skip_cows: a list of numbers
    """
    
    rng = wksht[f'{first_col}{start_row}:{last_col}{stop_row}']
    
    print(rng)
    print(rng[0])
    print(rng[0][0])
    quit()
    
    data = [[cell_.value
             for c_idx, cell_ in enumerate(row)
                 if c_idx not in skip_cols]  
             for r_idx, row in enumerate(rng)
                 if r_idx not in skip_rows]
    
    return data
    

def sp_loader(wksht, rows_no_update,
                act_key, end_key, first_col, last_col,
                skip_cols= [], column_names= [],
                yr_qtr_name= ''):
    '''
        read data from s&p excel workbook sheet
        that contains history for prices and earnings
        and that contains projections of future earnings
        return df
    '''
    
    # fetch data from wksht
    # fix the block of rows and cols that contain the data
    key_row = hp.find_key_row(wksht, 'A', 1, 
                              key_values= act_key)
    
    # first data row to read
    start_row = 1 + key_row
    
    # find the row with terminal key values, the extent of the data
    # move up from this row by amount = # row_no_update (hist data)
    # or 1 (proj data)
    # collect only the rows with new data
    stop_row = hp.find_key_row(wksht, 'A', start_row,
                               key_values= end_key,
                               is_stop_row= True)
    len_ = len(rows_no_update)
    bool_v = len_ > 0
    # adjust stop row for # rows not to update
    # if len_ > 0: subtract len_ from stop_row #
    # if len == 0: subtract 1 from stop_row #
    stop_row -= len_ * bool_v + 1 * (1 - bool_v)  

    if (stop_row < start_row):
        hp.message([
            'In sp_loader:',
            'the saved history has more quarters than the',
            'the history in the new file'
        ])
        quit()
    
    # fetch the data from the block
    # always reads at least one row
    data = data_block_reader(wksht, start_row, stop_row,
                             first_col, last_col, skip_cols)
    
    # row[0]: datetime or str, '%m/%d/%Y' is first 'word' in str
    # iterate over rows to convert all dates to datetime
    # this series is called 'date' in the pl.DF (must be one dtype)
    for row in data:
        row[0] = hp.dt_str_to_date(row[0])
        
    df = pl.DataFrame(data, schema=column_names, orient="row")\
                .cast({cs.float(): pl.Float32,
                       cs.datetime(): pl.Date})\
                .with_columns(pl.col('date')
                            .map_batches(hp.date_to_year_qtr)
                            .alias(yr_qtr_name))
                            
    return df


def margin_loader(wksht, dates_no_update,
                  row_key, first_col, stop_col_key,
                  stop_row_data_offset,
                  yr_qtr_name):
    '''
        read data from s&p excel worksheet
        that contains history for margins
        return df
    '''
    
    # find the rows with dates and data
    # start row contains dates
    start_row = hp.find_key_row(wksht, 'A', 1, key_values= [row_key])
    stop_row = start_row + stop_row_data_offset
    
    # fetch block of data from columns
    first_col = 'A'
    # if not fetching the full history
    if len(dates_no_update) > 0:
        last_col = 'C'
    else:
        # find last col with data, return number -> letter
        # years decrease as cols increase
        # start_col 1-based indexing col_num, default key is None
        last_col_num = hp.find_key_col(wksht, start_row,
                                       start_col= 3) - 1
        last_col = ut_cell.get_column_letter(last_col_num)

    # list of lists for each row, including headers for cols (start_row)
    data =  data_block_reader(wksht, start_row, stop_row,
                              first_col, last_col)
     
    # data_values omits the first row (col headers) from data
    data_values = [row for row in data[1:]]
    
    # omit the * for 2008, take first entry (yr) in list data[0]
    col_names = [str(item).split('*')[0] for item in data[0]]
    
    # build "tall" 2-col DF with 'year_qtr' and 'margin'
    df = pl.DataFrame(data_values, schema= col_names,
                      orient= 'row')\
                .with_columns(pl.col(row_key)
                              .map_elements(lambda x: x.split(' ')[0],
                                            return_dtype= str))\
                .cast({cs.float(): pl.Float32})\
                .unpivot(index= row_key, variable_name='year')
            # index: names of cols to remain cols
            # variable_name: name of col to contain names of cols pivoted
    
    df = df.with_columns(
                pl.struct([row_key, 'year'])\
                    .map_elements(lambda x: 
                                  (f"{x['year']}-{x[row_key]}"),
                                  return_dtype= pl.String)\
                    .alias(yr_qtr_name))\
            .drop(['year', row_key])\
            .rename({'value': 'op_margin'})
            
    return df


def industry_loader(wksht, years_no_update,
                    first_row_op, first_row_rep, num_inds,
                    start_col_key, stop_col_key):
    '''
        read data from s&p excel worksheet
        that contains history for industry data
        return df
    ''' 
    
## +++++ read industries from 1st col
    last_row_op = first_row_op + num_inds + 1
    ind = data_block_reader(wksht, first_row_op + 2, 
                            last_row_op, 'A', 'A')
    # remove parentheticals
    ind = [item[0].split(' (')[0]
           for item in ind]
    # remove 'S&P 500' and replace space with '_'
    ind_name = ['_'.join(item.rstrip().split(' ')[2:])
                for item in ind]
    # set first name to 'SP500'
    ind_name[0] = 'SP500'
                        
## +++++  read data  +++++
    # search the first row of data
    # 2nd block, bracketed by default key None
    # read the last three years of data
    
    # key value is None (col before 1st number to read)
    first_col_num = 1 + hp.find_key_col(wksht, first_row_op + 2,
                                        start_col= 3)
    # skip years not to update
    first_col_num += 2 * len(years_no_update)\
    
    first_col = ut_cell.get_column_letter(first_col_num)
    
    # key_value is None
    last_col_num = -1 + hp.find_key_col(wksht, first_row_op + 2, 
                                        start_col= first_col_num)
    
    last_col = ut_cell.get_column_letter(last_col_num)
   
## op data
    # fetch op e by industry, including row with headings
    # list of lists for each row
    data = data_block_reader(wksht, first_row_op, last_row_op,
                             first_col, last_col)
    
    date_data_names = [f'{name[:4]} {name[-3:]}'
                       for name in data[0]]
    
    columns_pe = [col
                  for col in date_data_names
                  if 'P/E' in col]
    columns_e = [col 
                 for col in date_data_names
                 if 'EPS' in col]
    years = [item[0:4] for item in columns_e]

    # use only rows with data
    gf = pl.DataFrame(data[2:], schema= date_data_names,
                      orient= 'row')\
           .cast({cs.float(): pl.Float32})
    
    # select and pivot to years for rows, data type for cols
    gf_pe = hp.gen_sub_df(gf, ind_name, '_op_pe', 
                          columns_pe, years)
    gf_e  = hp.gen_sub_df(gf, ind_name, '_op_eps', 
                          columns_e, years)
    
    df = gf_e.join(gf_pe,
                   on= 'year',
                   how= 'left',
                   coalesce= True)
    
## rep data
    last_row_rep = first_row_rep + num_inds - 1
    
    ## +++++  read data  +++++
    # fetch rep e by industry
    # list of lists for each row
    # no skips rows or year/type data
    data = data_block_reader(wksht, first_row_rep, last_row_rep,
                             first_col, last_col)

    gf = pl.DataFrame(data, schema= date_data_names,
                      orient= 'row')\
           .cast({cs.float(): pl.Float32})

    # select and pivot to years for rows, data type for cols
    gf_pe = hp.gen_sub_df(gf, ind_name, '_rep_pe', 
                          columns_pe, years)
    gf_e  = hp.gen_sub_df(gf, ind_name, '_rep_eps', 
                          columns_e, years)
    
    df = df.join(gf_e,
                   on= 'year',
                   how= 'left',
                   coalesce= True)\
           .join(gf_pe,
                 on= 'year',
                 how= 'left',
                 coalesce= True)
    
    return df


def fred_reader(wksht, start_row, stop_row,
                first_col, last_col,
                yr_qtr_name, rr_col_name):
    '''
        read data from FRED excel worksheet
        that contains history for real interest rates
        return df
    '''

    stop_row = wksht.max_row
    data = data_block_reader(wksht, start_row, stop_row,
                             first_col, last_col)
    
    if data[-1:][0][0] is None:
        hp.message([
            'In fred_reader:',
            f'None appears in the input data',
            f'last row is {data[-1]}'
        ])
        quit.exit()
    
    df = pl.DataFrame(data, schema=['date', rr_col_name],
                      orient='row')\
           .with_columns(pl.col('date')
                        .map_batches(hp.date_to_year_qtr)
                        .alias(yr_qtr_name))\
           .group_by(yr_qtr_name)\
           .agg([pl.all().sort_by('date').last()])\
           .sort(by= yr_qtr_name)\
           .drop('date')\
           .cast({cs.datetime(): pl.Date,
                  cs.float(): pl.Float32})
    return df

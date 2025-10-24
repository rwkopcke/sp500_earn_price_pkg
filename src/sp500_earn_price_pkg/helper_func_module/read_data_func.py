'''
   these are functions used by the update_data and display_data
   scripts
        
   access these values in other modules by
        import sp500_pe.read_data_func as rd
'''
from datetime import datetime


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
            'FILE_OUTPUT_WKBK_PREFIX} {DATE_FMT}.xlsx'
        where the date is taken from the
            SHT_EST_NAME sheet, near the top of WKBK_DATE_COL
        amends the names of sp files in input_dir
            to conform to the format above
        quit() if cannot find a date in the file
        Returns the set of std names
        
    https://www.programiz.com/python-programming/datetime/strftime
    https://duckduckgo.com/?q=python+string+to+datetime&t=osx&ia=web
    '''
    
    input_dir = env.INPUT_DIR
    # limit # of rows to read in WKBK_DATE_COL
    max = rd_param.MAX_DATE_ROWS
    date_col = rd_param.WKBK_DATE_COL
    
    output_sp_files_set = set()
    for file in names_set:
        path_name = input_dir / file
        sheet = find_wk_sheet(
            path_name,
            rd_param.SHT_EST_NAME)
        
        # returns a list of lists
        first_col_list = vals_from_row_col_array_in_xl_sheet(
            sheet, min_row= 1, max_row= max,
            start_col_ltr= date_col, 
            stop_col_ltr= date_col)
        
        # find file's date for file's name
        for row in first_col_list:
            
            # col_date is list of lists that contain 1-item each
            # cast_date returns value if not a date
            # if a date, returns date_string in DATE_FMT
            date_str = hp.cast_date_to_str(row[0])
            
            if hp.is_date(date_str, rd_param.DATE_FMT):
                datetime.strptime(date_str, rd_param.DATE_FMT)
                new_name_file = \
                    f'{env.FILE_OUTPUT_WKBK_PREFIX} {date_str}.xlsx'
                # add name to return set
                output_sp_files_set.add(new_name_file)
                # rename file
                path_name.rename(input_dir / new_name_file)
                break
        
        # full inspection of col_date_list w/o finding a date
        if not date_str:
            hp.message([
                'ERROR read_data_func.std_names for files',
                'found no date in first {max} rows of',
                f'{input_dir / file}',
                'inspect file for date'
                ])
            quit()
        
    return output_sp_files_set


def find_wk_sheet(file, sheet_name):
    '''
        Receive 
            path_name, address of xlsx
            sheet_name, name of sheet in xlsx
        Return sheet (openpyxl) for reading
    '''
    path_name = env.INPUT_DIR / file
    workbook = load_workbook(
            filename= path_name,
            read_only= True, 
            data_only= True)
    return workbook[sheet_name]


#def read_sheet_data(wksht,
def vals_from_row_col_array_in_xl_sheet(
        wksht, min_row= 1, max_row= 2,
        start_col_ltr= 'A', stop_col_ltr= 'B'):
    '''
        fetch data from an xls worsheet
        start_col and stop_col are letters
        return array of values as a pl.DataFrame
    '''

    return [[cell.value for cell in row]
             for row in wksht.iter_rows(
                min_row= min_row, 
                max_row= max_row,
                min_col= ut_cell\
                    .column_index_from_string(start_col_ltr), 
                max_col= ut_cell\
                    .column_index_from_string(stop_col_ltr))
            ]

def find_keys_in_xlsx(wksht,
                      row_num= 1,
                      col_ltr= 'A', 
                      keys_to_find= None,
                      is_row_iter= True,
                      cell_list= None):
    '''
        this function scans a row or a col of wksht to
            find the cell that displays an eligible key
        keys: either None or a list of strings,
        
        start at cell specified by col_ltr, row_num.
        if is_row_iter= True, iter "down" rows
        if False, iter across cols
        
        when a match occurs, return row_num or col_ltr
        when no match occurs, quit with message
    '''
    if cell_list is None:
        cell_list = []
    if not isinstance(cell_list, list):
        hp.message([
            'ERROR cell_list is not a list',
            'In helper_func.py, find_key_in_xlsx(),',
            f'cell_list: {cell_list}'])
        quit()
        
    # input array, convert dates to DATE_FMT atr
    
    if keys_to_find is not None:
        # then keys must be a list of str or []
        if ((type(keys_to_find) is not list)
            or
            (not all([type(item) is str 
                      for item in keys_to_find]))
            or
            (not keys_to_find)):
            hp.message([
                'ERROR keys is not a list of str',
                'In helper_func.py, find_key_in_xlsx(),',
                f'keys: {keys_to_find}'])
            quit()
    
    # initial values for iter loop
    _, col_num = coordinate_to_tuple(f'{col_ltr}{row_num}')
    
    if is_row_iter:
        max_row = wksht.max_row
        max_col = col_num
        max_to_read = max_row
        start_index = row_num
    else:
        max_col = wksht.max_column
        max_row = row_num
        max_to_read = max_col
        start_index = col_num

    max_to_read += 1
    
    # list of lists that each have 1 element
    if cell_list==[]:
        cell_list = \
            vals_from_row_col_array_in_xl_sheet(
                wksht, 
                min_row= row_num, max_row= max_row,
                start_col_ltr= col_ltr, 
                stop_col_ltr= 
                    ut_cell.get_column_letter(max_col))
    
        # if searching first col, cast dates to DATE_FMT
        if max_col == 1:
            # col_date is list of lists that contain 1-item each
            # cast dates to str; otherwise returns item as is
            cell_list = [hp.cast_date_to_str(row[0])
                        for row in cell_list]

    # col search produces a list with one element:
    # a list of the items in the search row
    if not is_row_iter:
        cell_list = cell_list[0]
    
    key_index = start_index
    # py list index is 0-based; excel rows/cols are 1-based
    for val in cell_list[start_index - 1: max_to_read]:
        if keys_to_find is None:
            if val is None:
                break
        else:
            if val in keys_to_find:
                break
        key_index += 1
    
    # if exhausted the list without finding key
    if key_index > (start_index + len(cell_list)):
        hp.message([
            f'In find_keys_in_xlsx',
            f'worksheet: {wksht}',
            f'start cell: {col_ltr}{row_num}',
            f'keys to find: {keys_to_find}',
            f'is row iteration: {is_row_iter}',
            f'list of values: {cell_list}',
            f'halting operation'
        ])
    
    if is_row_iter:
        return [key_index, cell_list]
    else:
        return [ut_cell.get_column_letter(key_index),
                cell_list]
    

def read_existing_history():
    '''
        Read saved history from parquet file, and
        Return as pl.df or empty pl.df
        
        Ensures 'date' col in pl.String in DATE_FMT
    '''
    if env.OUTPUT_HIST_ADDR.exists():
        return pl.read_parquet(env.OUTPUT_HIST_ADDR)\
                 .cast({'date': pl.String})
    else:
        return pl.DataFrame()
    

def find_qtrs_without_op_earn(df):
    '''
        Receives pl.df
        Return the set of rows to update
        If df is not empty
            return from col df[yr_qtrs]
            the set of yr_qtrs
            for which operating eps is null
        otherwise, return empty set
    '''
    
    if not df.is_empty():
        return set(
            pl.Series(df.filter(pl.col('op_eps').is_null())
                        .select(pl.col('date')))
                        .to_list())
    else:
        return set()  

    
def update_history(file, dates_set):
    '''
        Receives an xlsx sheet and dates
            to read from history on the sheet
        Reads two types of data from sheet:
            rectangular block of reported earn
            actual qtr-end prices since last
                qtr that contains reported earn
        Returns a pl.dataframe
            new historical data from the sheet
        sheet is a list of lists,
            a matrix extracted from the sheet
    '''
    sheet = find_wk_sheet(file, rd_param.SHT_EST_NAME)
    price = rd_param.PRICE_NAME
    date = rd_param.DATE_NAME
    
    if dates_set:
        stop_keys = [min(dates_set)]
    else:
        stop_keys = None

    # min_row and max_row are xl row indexes,
    # not py list indexes (0-based)
    [min_row, cell_list] = find_keys_in_xlsx(
            sheet,
            row_num= 1,
            col_ltr= rd_param.WKBK_DATE_COL,
            keys_to_find= rd_param.HISTORY_KEYS,
            cell_list= [])
    
    [max_row, _] = find_keys_in_xlsx(
            sheet,
            row_num= min_row,
            col_ltr= rd_param.WKBK_DATE_COL,
            keys_to_find= stop_keys,
            cell_list= cell_list)
    
    df = pl.DataFrame(vals_from_row_col_array_in_xl_sheet(
                            sheet, 
                            min_row= min_row + 1, 
                            max_row= max_row,
                            start_col_ltr= 
                                rd_param.HIST_EPS_COL_START, 
                            stop_col_ltr= 
                                rd_param.HIST_EPS_COL_STOP),
                orient= 'row')\
            .select(cs.by_dtype(pl.String, 
                            pl.Float64))\
            .cast({cs.float(): pl.Float32})\
            .with_columns(cs.first().map_elements(
                    hp.cast_date_to_str,
                    return_dtype= pl.String))
    df.columns = rd_param.HIST_COLUMN_NAMES
    
    recent_prices_df = pl.DataFrame(
            vals_from_row_col_array_in_xl_sheet(
                            sheet, 
                            min_row= min_row - 4, 
                            max_row= min_row - 2,
                            start_col_ltr= 
                               rd_param.PRICE_HST_COL_START, 
                            stop_col_ltr= 
                               rd_param.PRICE_HST_COL_STOP),
            schema = [date, price],
            orient= 'row')\
        .filter(~pl.col(price).is_null())\
        .with_columns(
            pl.col(date).map_elements(
                hp.cast_date_to_str,
                return_dtype= pl.String))\
        .cast( {price: pl.Float32})
        
    [key_row_num, _] = find_keys_in_xlsx(
            sheet,
            row_num= 1,
            col_ltr= rd_param.WKBK_DATE_COL,
            keys_to_find= rd_param.PRICE_KEYS,
            cell_list= cell_list)
    
    [date_, price_] = vals_from_row_col_array_in_xl_sheet(
                            sheet, 
                            min_row= key_row_num, 
                            max_row= key_row_num + 1,
                            start_col_ltr= 
                               rd_param.PRICE_RCNT_COL, 
                            stop_col_ltr= 
                               rd_param.PRICE_RCNT_COL)
    current_price_df = \
        pl.DataFrame({date: date_,
                      price: price_},
                     orient= 'row')\
          .with_columns(
              pl.col(date).map_elements(
                  hp.cast_date_to_str,
                  return_dtype= pl.String))\
          .cast({price: pl.Float32})
    
    recent_prices_df = pl.concat(
        [recent_prices_df, current_price_df],
        how= 'vertical')
        
    df = recent_prices_df.join(df, 
                               on= [price],
                               how= 'full',
                               coalesce=True,)\
            .with_columns(
                pl.when(pl.col(date).is_null())
                       .then(pl.col('date_right'))
                       .otherwise(pl.col(date))
                  .alias(date))\
            .drop(pl.col('date_right'))\
            .sort(by= date)\
            .with_columns(pl.col(date).map_batches(
                hp.date_to_year_qtr, 
                return_dtype= pl.String)
            .alias(rd_param.YR_QTR_NAME))
            
    if (any([item is None
            for item in df[date]])):
        hp.message([
            f'{file} \nmissing date for new entries',
            df[date]
        ])
        quit()

    return df


def fred_reader(file, dates_set):
    '''
        read data from FRED excel worksheet
        that contains history for real interest rates
            for dates in dates_set
        return df
    '''
    sheet = find_wk_sheet(file, rd_param.SHT_RR_NAME)
    
    [date, rr] = rd_param.RR_DF_SCHEMA
    yr_qtr = rd_param.YR_QTR_NAME
    min_yrqtr = hp.date_to_year_qtr(
        [hp.cast_date_to_str(min(dates_set))])
    
    # end of qtr data in Fred is dated with the
    # first day of the quarter => convert to yrqtr
    # and drop the dates asap
    df = pl.DataFrame(
            vals_from_row_col_array_in_xl_sheet(
                        sheet, 
                        min_row= rd_param.RR_MIN_ROW, 
                        max_row= sheet.max_row,
                        start_col_ltr= rd_param.RR_START_COL, 
                        stop_col_ltr= rd_param.RR_STOP_COL),
            schema = rd_param.RR_DF_SCHEMA,
            orient= 'row')\
        .filter(~pl.col(rr).is_null())\
        .cast({rr: pl.Float32})\
        .with_columns(
            pl.col(date).map_elements(
                hp.cast_date_to_str,
                return_dtype= pl.String))\
        .with_columns(pl.col(date)
                .map_batches(hp.date_to_year_qtr,
                             return_dtype= pl.String)
                .alias(yr_qtr))\
        .filter(pl.col(yr_qtr) >= min_yrqtr)\
        .group_by(yr_qtr)\
        .agg([pl.all().sort_by(date).last()])\
        .sort(by= yr_qtr)\
        .drop(pl.col(date))
        
    return df


def margin_loader(file, dates_set):
    '''
        read data from s&p excel worksheet
        that contains history for margins
        return df
    '''
    
    sheet = find_wk_sheet(file, rd_param.SHT_EST_NAME)
    
    [min_row, _] = find_keys_in_xlsx(sheet,
                          row_num= 1, 
                          col_ltr= 'A', 
                          keys_to_find= rd_param.MARG_KEYS)
    [stop_col_ltr, _] = find_keys_in_xlsx(sheet,
                          row_num= min_row, 
                          col_ltr= 'A', 
                          keys_to_find= None,
                          is_row_iter= False)
    
    stop_col_ltr = ut_cell.get_column_letter(
        ut_cell.column_index_from_string(stop_col_ltr) - 1)
    
    # list of lists
    data = vals_from_row_col_array_in_xl_sheet(
                            sheet, 
                            min_row= min_row, 
                            max_row= min_row + 4,
                            start_col_ltr= 
                                rd_param.MARG_KEY_COL, 
                            stop_col_ltr= stop_col_ltr)
     
    # data_values omits the first row (col headers) from data
    data_values = [row for row in data[1:]]
    
    # omit the * for 2008, take first entry (yr) in list data[0]
    col_names = [str(item).split('*')[0] for item in data[0]]
    qtrs = col_names[0]
    
    # build "tall" 2-col DF with 'year_qtr' and 'margin'
    df = pl.DataFrame(data_values, 
                      schema= col_names,
                      orient= 'row')\
                .with_columns(pl.col(qtrs)
                              .map_elements(lambda x: x.split(' ')[0],
                                            return_dtype= str))\
                .cast({cs.float(): pl.Float32})\
                .unpivot(index= qtrs, variable_name='year')
            # index: names of cols to remain cols
            # variable_name: name of col to contain names of cols pivoted
    
    df = df.with_columns(
                pl.struct([qtrs, 'year'])\
                    .map_elements(lambda x: 
                                  (f"{x['year']}-{x[qtrs]}"),
                                  return_dtype= pl.String)\
                    .alias(rd_param.YR_QTR_NAME))\
            .drop(['year', qtrs])\
            .rename({'value': 'op_margin'})
            
    return df

    
#$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$

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
    
    data = [[cell_.value
             for c_idx, cell_ in enumerate(row)
                 if c_idx not in skip_cols]  
             for r_idx, row in enumerate(rng)
                 if r_idx not in skip_rows]
    
    return data


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


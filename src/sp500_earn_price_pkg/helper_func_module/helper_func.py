'''
   these are functions used by the update_data and display_data
   scripts and by the read_data_func functions
        
   access these values in other modules by
        import sp500_pe.helper_func as hp
'''
from datetime import datetime
import polars as pl

import sp500_earn_price_pkg.config.set_params as params
rd_param = params.Update_param()


def message(msg):
    '''
        template for printing msg, a list of strings to terminal,
        one string per line.
        returns nothing
    '''
    num = 100
    print('\n')
    print('='*num)
    for line in msg:
        print(line)
    print('='*num, '\n')
    

def cast_date_to_str(val):
    '''
        if val is either a 
            datetime object or contains
            a date as str, in format: rd_param.DATE_FMT_SP_ITEM
        return date as str, in format: rd_param.DATE_FMT_SP_FILE
        
        Otherwise, return ""
    '''
    if (isinstance(val, datetime)):
        return val.strftime(rd_param.DATE_FMT_SP_FILE)
    
    # if val contains valid str date, convert to datetime
    if isinstance(val, str):
        val = val[:10]         #isolates the date, if exists
        date_ = is_str_a_date(val, rd_param.DATE_FMT_SP_ITEM)
        if date_:
            return date_.strftime(rd_param.DATE_FMT_SP_FILE)
    return ""
        

def is_str_a_date(val, date_fmt):
    '''
        Receive str and date_format str
        If val is a str that can be cast to a date
            Return the datetime obj
        Otherwise,
            Return empty str obj
    '''
    try:
        return datetime.strptime(val, date_fmt)
    except:
        return ""
    
    
def file_to_date_str(series):
    '''
        receives pl.Series (col from df), str file names
        extracts the date string, rd_param.DATE_FMT_SP_FILE,
        returns: date object, as pl.Series
    '''
    # isolate the date str in f_name then -> date
    return pl.Series(
        [(f_name.split(' ', 1)[1]).split('.', 1)[0]
         for f_name in series])
    
    
def date_to_year_qtr(series):
    '''
        Receives pl.Series (col from df) of date_str
        Returns year_qtr str, yyyy-Qq, as pl.Series
    '''
    return pl.Series([f"{date[:4]}-Q{date_to_qtr(date)}"
                     for date in series])
    
    
def date_to_qtr(date):
    '''
        Extracts month from date_str
        Returns qtr number as string
    '''
    return f"{((int(date[6:7])) - 1) // 3 + 1 }"


def is_quarter_4(series):
    '''
        returns bool: T if qtr == 4; else F
    '''
    return pl.Series([yq[-1] == '4'
                      for yq in series])
    
    
def yrqtr_to_yr(series):
    '''
        series of strings y-q in,
        series of strings y out
    '''
    return pl.Series([yq[:4]
                      for yq in series])


def gen_sub_df(df, ind_name, suffix, col_select, years):
    '''
        describe
    '''

    # construct list of industries: "row index"
    cols = [item + suffix
            for item in ind_name]
    # one-col DF
    col_names = pl.DataFrame(cols, schema= ['IND'])
    
    # filter cols of df
    gf = df.select(col_select)
    # rename cols of gf to simply years
    gf.columns = years
    # add col of col_names to gf
    # pivot industries to become new cols
    gf = pl.concat([col_names, gf], 
                   how= 'horizontal')\
           .unpivot(index= 'IND', variable_name= 'year')\
           .pivot(on= 'IND', values= 'value')
    return gf


def my_df_print(df, n_rows=30):
    '''
        custom print df function to format data
    '''                                
    with pl.Config(
        tbl_cell_numeric_alignment="RIGHT",
        thousands_separator=",",
        float_precision=1,
        tbl_rows = n_rows
    ):
        print(df)
    return
    

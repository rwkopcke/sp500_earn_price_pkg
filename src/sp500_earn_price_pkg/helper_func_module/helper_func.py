'''
   these are functions used by the update_data and display_data
   scripts and by the read_data_func functions
        
   access these values in other modules by
        import sp500_pe.helper_func as hp
'''
from datetime import datetime
import polars as pl

import config.set_params as params

param = params.Update_param()
year_col_name = param.ANNUAL_DATE
ind_col_name = param.IND_COL_NAME

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
            a date as str, in format: param.DATE_FMT_SP_WKBK
        return date as str, in format: param.DATE_FMT
        
        Otherwise, return val
    '''
    if isinstance(val, datetime):
        return val.strftime(param.DATE_FMT)
    
    # if val contains valid str date, convert to datetime
    if isinstance(val, str):
        #isolates the date, if exists  
        return str_to_date(val, 
                        param.DATE_FMT_SP_WKBK,
                        param.DATE_FMT)
        # either date as str
    return val
        

def str_to_date(val, date_fmt_wkbk, date_fmt):
    '''
        Receive datetime and date_format str
        If val is str that can be cast to a date
            using date_fmt,
        Return date as str
            using param.DATE_FMT
        Otherwise,
            Return empty str obj
    '''
    val_ = val.split(" ")[0]
    try:
        date_ = datetime.strptime(val_, date_fmt_wkbk)
        return datetime.strftime(date_, date_fmt)
    except Exception as e:
        return val
    
    
def str_is_date(val, proj_date_fmt):
    '''
        checks if:
            val is str that matches DATE_FMT
            can be interpreted as a valid datetime obj
        if so, returns True; otherwise, False
    '''
    try:
        val == \
        datetime.strptime(val, proj_date_fmt)\
                .strftime(proj_date_fmt)
        return True
    except Exception as e:
        return False
    
    
def file_to_date_str(series):
    '''
        Receives pl.Series (col from df), str file names
        Extracts the date string from param.DATE_FMT,
        Returns: date as str object, as pl.Series
    '''
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
    return f"{((int(date[5:7])) - 1) // 3 + 1 }"


def is_quarter_4(series):
    '''
        Receives date as str pl.Series from pl.DF['date']
        Returns bool: T if qtr == 4; else F
    '''
    return pl.Series([yr_qtr[-1] == '4'
                      for yr_qtr in series])
    
    
def yrqtr_to_yr(series):
    '''
        Receives date as str pl.Series from pl.DF['date']
        Returns year as str
    '''
    return pl.Series([yr_qtr[:4]
                      for yr_qtr in series])
    

def convert_date_str_to_wkbk_fmt(date):
    '''
        Receives date as a str in project format
        Returns date as a str in wkbk formot
    '''
    print(date, type(date))
    return (f'{date[8:10]}{param.DATE_SP_WKBK_SEP}' +
            f'{date[-5:-3]}{param.DATE_SP_WKBK_SEP}' +
            f'{date[0:4]}')


def transpose_df(df, ind_name, earn_metric,
                index_tyoe, e_type,col_select, years):
    '''
        transposes df and
        add cols for 
            SP index, type of earnings, measure of earnings
    '''

    # twp-col DF ind, containing df's col names 
    col_names = pl.DataFrame(ind_name, 
                             schema= ind_col_name)
    
    # filter cols of df, eps or pe
    gf = df.select(col_select)
    # rename cols of gf, using years
    gf.columns = years
    # add col of col_names to gf
    # melt cols into one col of years, one col of values
    # pivot ind into cols with values from 'value'
    gf = pl.concat([col_names, gf], 
                   how= 'horizontal')\
           .unpivot(index= ind_col_name, variable_name= year_col_name)\
           .pivot(on= ind_col_name, values= 'value')\
           .with_columns(pl.lit(index_tyoe)
                           .alias(param.IDX_COL_NAME),
                         pl.lit(e_type)
                           .alias(param.EARNINGS_COL_NAME),
                         pl.lit(earn_metric)
                           .alias(param.E_METRIC_COL_NAME))
    return gf


def my_df_print(df, n_rows=40):
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

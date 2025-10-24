from dataclasses import dataclass

@dataclass(frozen= True, slots= True)
class Update_param:
    ARCHIVE_RR_FILE = False
    
# format for dates as str in sp xlsx file names
    DATE_FMT = '%Y-%m-%d'
    DATE_FMT_SP_WKBK = '%m/%d/%Y'
    DATE_SP_WKBK_SEP = '/'
    DATE_FMT_SEP = '-'
    
# date of wkbk
    WKBK_DATE_COL = 'A'
    # max rows to read to find the date of a worksheet
    MAX_DATE_ROWS = 5
    
# name of sheet with history and proj data
    SHT_EST_NAME = 'ESTIMATES&PEs'

# data for sp files and actual df
    DATE_NAME = 'date'
    PRICE_NAME = 'price'
    RR_NAME = 'real_int_rate'
    YR_QTR_NAME = 'yr_qtr'
    PROJ_PREFIX_OUTPUT_FILE = 'sp-500-eps-est'
    EXT_OUTPUT_FILE_NAME = '.parquet'
    
    # 'date' is str (DATE_FMT_SP_FILE); the rest are Float32
    HIST_COLUMN_NAMES = [DATE_NAME, PRICE_NAME, 'op_eps', 'rep_eps',
                'op_p/e', 'rep_p/e', '12m_op_eps', '12m_rep_eps']
    PROJ_COLUMN_NAMES = [DATE_NAME, 'op_eps', 'rep_eps',
                        'op_p/e', 'rep_p/e', '12m_op_eps', '12m_rep_eps']
    
# keys for history eps and price
    HISTORY_KEYS = ['ACTUALS', 'Actuals']
    HIST_EPS_COL_START = 'A'
    HIST_EPS_COL_STOP = 'J'
    
    PRICE_KEYS = ['Date', 'Data as of the close of:']
    PRICE_HST_COL_START = 'A'
    PRICE_HST_COL_STOP = 'B'
    PRICE_RCNT_COL = 'D'
    
    ESTIMATES_KEYS = ['ESTIMATES']
    END_KEY = ['END']
    
# data from FRED wksht
    SHT_RR_NAME = 'Quarterly'
    RR_MIN_ROW = 2
    RR_START_COL = 'A'
    RR_STOP_COL = 'B'
    RR_DF_SCHEMA = [DATE_NAME, RR_NAME]
    
# data for margins
    MARG_KEYS = ['QTR']
    MARG_KEY_COL= 'A'
    MARG_MAX_ROW_OFFSET= 4
    MARG_START_COL = 'B'
    MARG_STOP_COL_KEY= None

# data from sp quarterly wksht
    SHT_QTR_NAME = "QUARTERLY DATA"
    QTR_COLUMN_NAMES = [DATE_NAME, 'div_ps', 'sales_ps',
                        'bk_val_ps', 'capex_ps', 'divisor']

# data from sp industry wksht
    SHT_IND_NAME = 'SECTOR EPS'
    
    # NB ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    # all search (row or col) "keys" should be None or lists
    # all column indexes in skip lists below are zero-based ('A' is 0)
    # all specific individual column designations are letters
    # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    

    SHT_IND_PARAMS = {
        'first_row_op': 6,
        'first_row_rep': 63,
        'num_inds': 12,
        'start_col_key': None,
        'stop_col_key': None
    }

    SHT_QTR_PARAMS = {
        'act_key' : END_KEY,
        'end_key' : None,
        'first_col' : 'A',
        'last_col' : 'I',
        'skip_cols' : [1, 2, 7],
        'column_names' : QTR_COLUMN_NAMES,
        'yr_qtr_name' : YR_QTR_NAME
    }

    SHT_EST_PROJ_DATE_PARAMS = {
        'date_keys' :  PRICE_KEYS,
        'value_col_1' : 'D', 
        'date_key_2' : None, 
        'value_col_2' : None,
        'column_names' : None,
        'yr_qtr_name' : YR_QTR_NAME
    }

    SHT_EST_PROJ_PARAMS = {
        'act_key' : ESTIMATES_KEYS,
        'end_key' : HISTORY_KEYS,
        'first_col' : 'A',
        'last_col' : 'J',
        'skip_cols' : [1, 4, 7],
        'column_names' : PROJ_COLUMN_NAMES,
        'yr_qtr_name' : YR_QTR_NAME
    }
    

@dataclass(frozen= True, slots= True)
class Display_param:
    # main titles for displays
    PAGE0_SUPTITLE = " \nCalendar-Year Earnings per Share for the S&P 500"
    PAGE1_SUPTITLE = " \nRatio of Price to Trailing 4-Quarter Earnings for the S&P 500"
    PAGE2_SUPTITLE = " \nEarnings Margin and Equity Premium for the S&P 500"
    PAGE3_SUPTITLE = \
        " \nS&P 500 Forward Earnings Yield, 10-Year TIPS Rate, and Equity Premium"

    # str: source footnotes for displays
    E_DATA_SOURCE = \
        '''
        https://www.spglobal.com/spdji/en/search/?query=index+earnings&activeTab=all
                                       (EPSEST)
        '''
    RR_DATA_SOURCE = '10-year TIPS: latest rate for each quarter,' + \
        ' Board of Governors of the Federal Reserve System, ' + \
        '\nMarket Yield on U.S. Treasury Securities at 10-Year' + \
        ' Constant Maturity, Investment Basis, Inflation-Indexed,' +\
        '\nfrom Federal Reserve Bank of St. Louis, FRED [DFII10].'
    PAGE0_SOURCE = E_DATA_SOURCE
    PAGE1_SOURCE = E_DATA_SOURCE
    PAGE2_SOURCE = E_DATA_SOURCE + '\n\n' + RR_DATA_SOURCE
    PAGE3_SOURCE = E_DATA_SOURCE + '\n\n' + RR_DATA_SOURCE

    # hyopothetical quarterly growth factor future stock prices
    ROG = .05
    ROG_AR = int(ROG * 100)
    ROGQ = (1. + ROG) ** (1/4)

    HIST_COL_NAMES = ['date', 'yr_qtr', 'price', 'op_eps', 'rep_eps',
                    'op_p/e', 'rep_p/e', '12m_op_eps', '12m_rep_eps',
                    'op_margin', 'real_int_rate']

    DATA_COLS_RENAME  = {'op_margin': 'margin',
                        'real_int_rate': 'real_rate'}
    
@dataclass(frozen= True, slots= True)
class Display_ind_param:
    # main titles for displays
    PAGE4_SUPTITLE = "\nOperating Price-Earnings Ratios for " +\
        "the Industries Within the S&P 500"
    PAGE5_SUPTITLE = \
        "\nCorrelations among Annual Price-Earnings Ratios \nfor " +\
        "the Industries Within the S&P 500"
    PAGE6_SUPTITLE = \
        "\nEach Industry's Share of Total Earnings for the Industries in the S&P 500"

    # str: source footnotes for displays
    E_DATA_SOURCE = \
        'https://www.spglobal.com/spdji/en/search/?query=index+earnings&activeTab=all'
    RR_DATA_SOURCE = '10-year TIPS: latest rate for each quarter,' + \
        ' Board of Governors of the Federal Reserve System, ' + \
        '\nMarket Yield on U.S. Treasury Securities at 10-Year' + \
        ' Constant Maturity, Investment Basis, Inflation-Indexed,' +\
        '\nfrom Federal Reserve Bank of St. Louis, FRED [DFII10].\n '
    PAGE4_SOURCE = '\n' + E_DATA_SOURCE + '\n' +\
        "NB: S&P calculates the index of earnings for the S&P 500 " +\
        "differently than earnings for the industries.\n" +\
        "The index of earnings for the S&P 500 usually is more than twice the sum of " +\
        "earnings for the industries. The S&P 500's P/E is not the " +\
        "average of the industries' P/Es.\n "
    PAGE5_SOURCE = '\n' + E_DATA_SOURCE

    XLABL = 'end of year'
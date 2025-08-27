from dataclasses import dataclass

@dataclass(frozen= True)
class Update_param:
    ARCHIVE_RR_FILE = False

    # data from "ESTIMATES&PEs" wksht
    RR_COL_NAME = 'real_int_rate'
    YR_QTR_NAME = 'yr_qtr'
    PREFIX_OUTPUT_FILE_NAME = 'sp-500-eps-est'
    EXT_OUTPUT_FILE_NAME = '.parquet'

    SHT_EST_NAME = "ESTIMATES&PEs"
    COLUMN_NAMES = ['date', 'price', 'op_eps', 'rep_eps',
                    'op_p/e', 'rep_p/e', '12m_op_eps', '12m_rep_eps']
    PROJ_COLUMN_NAMES = ['date', 'op_eps', 'rep_eps',
                        'op_p/e', 'rep_p/e', '12m_op_eps', '12m_rep_eps']

    SHT_QTR_NAME = "QUARTERLY DATA"
    COLUMN_NAMES_QTR = ['date', 'div_ps', 'sales_ps',
                        'bk_val_ps', 'capex_ps', 'divisor']

    SHT_IND_NAME = 'SECTOR EPS'
    
    # NB ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    # all search (row or col) "keys" should be None or lists
    # all column indexes in skip lists below are zero-based ('A' is 0)
    # all specific individual column designations are letters
    # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

    ACTUAL_KEYS = ['ACTUALS', 'Actuals']
    
    SHT_EST_DATE_PARAMS = {
        'date_keys' : ['Date', 'Data as of the close of:'],
        'value_col_1' : 'D',
        'date_key_2' : ACTUAL_KEYS,
        'value_col_2' : 'B',
        'column_names' : COLUMN_NAMES,
        'yr_qtr_name' : YR_QTR_NAME
    }

    SHT_HIST_PARAMS = {
        'act_key' : ACTUAL_KEYS,
        'end_key' : None,
        'first_col' : 'A',
        'last_col' : 'J',
        'skip_cols' : [4, 7],
        'column_names' : COLUMN_NAMES,
        'yr_qtr_name' : YR_QTR_NAME
    }

    MARG_KEY = 'QTR'
    SHT_BC_MARG_PARAMS = {
        'row_key': MARG_KEY,
        'first_col': 'A',
        'stop_col_key': None,
        'stop_row_data_offset': 4,
        'yr_qtr_name': YR_QTR_NAME
    }

    SHT_IND_PARAMS = {
        'first_row_op': 6,
        'first_row_rep': 63,
        'num_inds': 12,
        'start_col_key': None,
        'stop_col_key': None
    }

    SHT_QTR_PARAMS = {
        'act_key' : ['END'],
        'end_key' : None,
        'first_col' : 'A',
        'last_col' : 'I',
        'skip_cols' : [1, 2, 7],
        'column_names' : COLUMN_NAMES_QTR,
        'yr_qtr_name' : YR_QTR_NAME
    }

    SHT_EST_PROJ_DATE_PARAMS = {
        'date_keys' : ['Date', 'Data as of the close of:'],
        'value_col_1' : 'D', 
        'date_key_2' : None, 
        'value_col_2' : None,
        'column_names' : None,
        'yr_qtr_name' : YR_QTR_NAME
    }

    SHT_EST_PROJ_PARAMS = {
        'act_key' : ['ESTIMATES'],
        'end_key' : ACTUAL_KEYS,
        'first_col' : 'A',
        'last_col' : 'J',
        'skip_cols' : [1, 4, 7],
        'column_names' : PROJ_COLUMN_NAMES,
        'yr_qtr_name' : YR_QTR_NAME
    }

    SHT_FRED_PARAMS = {
        'first_row': 12,
        'col_1': 'A',
        'col_2': 'B',
        'yr_qtr_name': YR_QTR_NAME,
        'rr_col_name': RR_COL_NAME
    }

@dataclass(frozen= True)
class Display_param:
    # main titles for displays
    PAGE0_SUPTITLE = " \nPrice-Earnings Ratios for the S&P 500"
    PROJ_EPS_SUPTITLE = " \nCalendar-Year Earnings per Share for the S&P 500"
    PAGE2_SUPTITLE = " \nEarnings Margin and Equity Premium for the S&P 500"
    PAGE3_SUPTITLE = \
        " \nS&P 500 Forward Earnings Yield, 10-Year TIPS Rate, and Equity Premium"

    # str: source footnotes for displays
    E_DATA_SOURCE = \
        'https://www.spglobal.com/spdji/en/search/?query=index+earnings&activeTab=all'
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
    
@dataclass(frozen= True)
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
import polars as pl
from dataclasses import dataclass

from config import config_paths as env


@dataclass(frozen= True, slots= True)
class Update_param:
    ARCHIVE_RR_FILE = False

# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#           DATES FORMATS
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    DATE_FMT = '%Y-%m-%d'          # fmt for this project
    DATE_FMT_SP_WKBK = '%m/%d/%Y'  # fmt on sp xlsx
    DATE_SP_WKBK_SEP = '/'
    DATE_FMT_SEP = '-'

# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#           ENUMS FOR POLARS (categorical variables)
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    # year range - ENSURE THAT THIS RANGE COVERS ALL YEARS of DATA
    YEAR_ENUM = pl.Enum([str(yr) for yr in range(1980, 2101)])

    # Earnings types and guages to collect
    EARN_TYPES = ['op', 'rep']
    EARN_METRICS = ['eps', 'p/e']
    EARN_TYPE_ENUM = pl.Enum(EARN_TYPES)
    EARN_METRIC_ENUM = pl.Enum(EARN_METRICS)

    # SP Indexes to collect
    SP500  = 'SP500'
    SP400  = 'SP400'
    SP600  = 'SP600'
    SP1500 = 'SP1500'
    SP_IDX_TYPES = [SP500, SP400, SP600, SP1500]
    INDEX_ENUM = pl.Enum(SP_IDX_TYPES)

# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#           DF COL NAMES
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    # Actual DF
    DATE_NAME = 'date'
    YR_QTR_NAME = 'yr_qtr'
    ANNUAL_DATE = 'year'
    PRICE_NAME = 'price'
    RR_NAME = 'real_int_rate'
    
    OP_EPS = 'op_eps'
    OP_PE = 'op_pe'
    REP_EPS = 'rep_eps'
    REP_PE = 'rep_pe'
    
    ANN_OP_EPS = '12m_' + OP_EPS
    ANN_REP_EPS = '12m_' + REP_EPS
    
    # names for other qtrly data
    DIV_PS = 'div_ps'
    SALES_PS = 'sales_ps'
    BOOK_PS = 'bk_val_ps'
    CAPEX_PS = 'capex_ps'
    DIVISOR = 'divisor'

    # Ind DF
    IDX_E_COL_NAME = 'index'   # E for the index
    E_TYPE_COL_NAME = 'earnings_type'
    E_METRIC_COL_NAME = 'earnings_metric'
    IDX_TYPE_COL_NAME = 'index_type'
    
    # Ind DF
    # for read_data.py, industry_loader(), to update ind name
    TELECOM_SERV = 'Telecommunication_Services'
    COM_SERV = 'Communication_Services'
    E_COLS_DROP = ['Real_Estate']
    
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#           KEYS AND PARAMS for extracting the data from s&P xlsx
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    # history and project df
    # name of sheet with history and proj data
    SHT_EST_NAME = 'ESTIMATES&PEs'
    
    # to find date of sp input wkbks
    WKBK_DATE_COL = 'A'
    # max rows to read to find the date of a worksheet
    MAX_DATE_ROWS = 15
    
    # cols for sp500 E analysis
    HIST_COLUMN_NAMES = [DATE_NAME, YR_QTR_NAME, PRICE_NAME, OP_EPS, 
                         REP_EPS, OP_PE, REP_PE, ANN_OP_EPS, ANN_REP_EPS]
    MARG_COL_NAME = 'op_margin'
    RR_COL_NAME = RR_NAME
    RR_DF_SCHEMA = [DATE_NAME, RR_NAME]
    
    # cols for other history data
    SHT_QTR_NAME = "QUARTERLY DATA"
    QTR_COLUMN_NAMES = [DATE_NAME, DIV_PS, SALES_PS,
                        BOOK_PS, CAPEX_PS, DIVISOR]
    
    # cols for projections
    PROJ_COLUMN_NAMES = [DATE_NAME, YR_QTR_NAME, OP_EPS, REP_EPS,
                        OP_PE, REP_PE, ANN_OP_EPS, ANN_REP_EPS]

    # cols for industry data
    SHT_IND_NAME = 'SECTOR EPS'
    # take col names from DF in ind parquet file
    
    # Keys for history eps and price
    HISTORY_KEYS = ['ACTUALS', 'Actuals', 'ACTUAL']
    PRICE_OFFSET_1 = 4
    PRICE_OFFSET_2 = 2
    
    HIST_EPS_COL_START = 'A'
    HIST_EPS_COL_STOP = 'J'
    
    PRICE_KEYS = ['Date', 'Data as of the close of:']
    PRICE_HST_COL_START = 'A'
    PRICE_HST_COL_STOP = 'B'
    PRICE_RCNT_COL = 'D'
    
    # Keys for margins
    MARG_KEYS = ['QTR']
    MARG_KEY_COL= 'A'
    MARG_KEY_POS= 0
    MARG_MAX_ROW_OFFSET= 4
    MARG_STOP_COL_KEY= 'None'
    
    # Keys for estimates
    ESTIMATES_KEYS = ['ESTIMATES']
    END_KEY = ['END']
    
    PROJ_MAX_LIST = 140
    PROJ_ROW_START_KEYS = ESTIMATES_KEYS
    PROJ_ROW_STOP_KEYS = None
    PROJ_COLS_TO_SKIP = [1, 4, 7]

    # data from sp quarterly wksht
    QTR_INIT_ROW_ITER = 10
    QTR_START_KEYS = ['END']
    QTR_HST_COL_START = 'A'
    QTR_HST_COL_STOP = 'I'
    QTR_COL_TO_SKIP = [1, 2, 7]
    
    # data from FRED wksht
    SHT_RR_NAME = 'Quarterly'
    RR_MIN_ROW = 2
    RR_START_COL = 'A'
    RR_STOP_COL = 'B'
    # should be less than yr_qtr than any data from FRED
    RR_MIN_YR_QTR = '2000-Q1'

    # data from sp industry wksht
    IDX_OFFSET  = 13  # for "sector eps" sheet

    IND_SRCH_COL = 'A'
    IND_INIT_ROW_ITER = 30
    
    IND_OP_START_KEYS = ['S&P 500']
    IND_OP_STOP_KEYS = None
    IND_OFFSET = -2
    
    OP_REP_OFFSET = 55
    
    IND_DATA_FIRST_COL_KEY = ['2008 EPS']
    IND_DATA_LAST_COL_KEY = None
    IND_DATA_START_COL = 'D'
    IND_DATA_START_COL_OFFSET = 3
    
    EPS_MK = 'EPS'
    PE_MK = 'P/E'
    

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
    ROG = env.ENVIRONMENT_DICT.get("rate_of_growth_of_sp_index", 0.05)
    ROG_AR = int(ROG * 100)
    ROGQ = (1. + ROG) ** (1/4)

    HIST_COL_NAMES = [*Update_param().HIST_COLUMN_NAMES,
                     Update_param().MARG_COL_NAME,
                     Update_param().RR_NAME]

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
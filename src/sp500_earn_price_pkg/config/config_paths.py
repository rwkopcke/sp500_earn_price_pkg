'''
    set global absolute paths for dirs and file addresses
        using path to "sp500_earn_price_pkg" project's directory
    create universal path expressions using Path() from pathlib
        
    these addresses are attributes of params, 
        an instance of a frozen dataclass, Fixed_locations
    params is accessed from other scripts by 
        from sp500_earn_price_pkg.config_paths import params
'''

from pathlib import Path
from dataclasses import dataclass


@dataclass(frozen= True, slots= True)
class Fixed_locations:
    # source of new data, recorded in the output file
    # '.../sp500_earn_price_pkg/input_output/output_dir/record_dict.json'
    SP_SOURCE = \
        "https://www.spglobal.com/spdji/en/search/?query=index+earnings&activeTab=all"
    REAL_RATE_SOURCE = "https://fred.stlouisfed.org/series/DFII10"
    
    # keys without values for record_dict
    RECORD_DICT_TEMPLATE = \
        {'sources': {'s&p': '',             
                     'tips': ''},
        'latest_file': {"yr_qtr": '',
                        "file": '',
                        "date": ''},       # value is str, json not accept date
        'quarters in record': [],
        'prev_used_files': {"yr_qtr": '',
                            "file": '',
                            "date": ''}    # value is str, json not accept date
        }
    
    # root for project, top-level proj directory
    BASE_DIR = Path.cwd()
    
    # fixed address, not in project's directory
    ARCHIVE_DIR = \
    Path('/Users/richardkopcke/Dropbox/Stock Analysis/sp_data_archive')
    
## ====================================================================
## ========== Set paths and addr within proj's dir ====================
## ====================================================================
    INPUT_OUTPUT_DIR = BASE_DIR / "input_output"
    
    INPUT_DIR = INPUT_OUTPUT_DIR / "input_dir"
    # to reinitialize all data in the project, using all archived history:
    #    put # before the statement for INPUT_DIR immed. above
    #    remove # before INPUT_DIR = ARCHIVE_DIR below
    #    make sure 'DFII10.xlsx' is in ARCHIVE_DIR
    #    reverse these steps to return to normal operation
    # INPUT_DIR = ARCHIVE_DIR
    
    # ensure this file is in INPUT_DIR (see above)
    INPUT_RR_FILE = 'DFII10.xlsx'
    INPUT_RR_ADDR = INPUT_DIR / INPUT_RR_FILE
    
    RECORD_DICT_DIR = INPUT_OUTPUT_DIR
    RECORD_DICT_FILE = "record_dict.json"
    RECORD_DICT_ADDR = RECORD_DICT_DIR / RECORD_DICT_FILE
    
    INPUT_SP_FILE_GLOB_STR = 'sp-500*.xlsx'
    
    OUTPUT_DIR = INPUT_OUTPUT_DIR / "output_dir"
    
    OUTPUT_HIST_FILE = 'sp500_pe_df_actuals.parquet'
    OUTPUT_HIST_ADDR = OUTPUT_DIR / OUTPUT_HIST_FILE

    OUTPUT_IND_FILE = 'sp500_ind_df.parquet'
    OUTPUT_IND_ADDR = OUTPUT_DIR / OUTPUT_IND_FILE
    
    OUTPUT_PROJ_FILE = 'sp500_pe_df_estimates.parquet'
    OUTPUT_PROJ_ADDR = OUTPUT_DIR / OUTPUT_PROJ_FILE

    BACKUP_DIR = INPUT_OUTPUT_DIR / 'backup_dir'
    
    BACKUP_HIST_FILE = "backup_pe_df_actuals.parquet"
    BACKUP_HIST_ADDR = BACKUP_DIR / BACKUP_HIST_FILE
    
    BACKUP_IND_FILE  = "backup_ind_df.parquet"
    BACKUP_IND_ADDR  = BACKUP_DIR / BACKUP_IND_FILE
    
    BACKUP_PROJ_FILE  = "backup_pe_estimates_df.parquet"
    BACKUP_PROJ_ADDR  = BACKUP_DIR / BACKUP_PROJ_FILE
    
    BACKUP_RECORD_DICT = "backup_record_dict.json"
    BACKUP_RECORD_DICT_ADDR = BACKUP_DIR / BACKUP_RECORD_DICT

    DISPLAY_DIR = INPUT_OUTPUT_DIR / "display_dir"
    DISPLAY_0 = 'eps_page0.pdf'
    DISPLAY_1 = 'eps_page1.pdf'
    DISPLAY_2 = 'eps_page2.pdf'
    DISPLAY_3 = 'eps_page3.pdf'
    DISPLAY_4 = 'eps_page4.pdf'
    DISPLAY_5 = 'eps_page5.pdf'
    DISPLAY_6 = 'eps_page6.pdf'
    DISPLAY_0_ADDR = DISPLAY_DIR / DISPLAY_0
    DISPLAY_1_ADDR = DISPLAY_DIR / DISPLAY_1
    DISPLAY_2_ADDR = DISPLAY_DIR / DISPLAY_2
    DISPLAY_3_ADDR = DISPLAY_DIR / DISPLAY_3
    DISPLAY_4_ADDR = DISPLAY_DIR / DISPLAY_4
    DISPLAY_5_ADDR = DISPLAY_DIR / DISPLAY_5
    DISPLAY_6_ADDR = DISPLAY_DIR / DISPLAY_6

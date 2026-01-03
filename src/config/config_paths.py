'''
    set global absolute paths for dirs and file addresses
        using path to "sp500_earn_price_pkg" project's directory
    create universal path expressions using Path() from pathlib
        
    these addresses are attributes of params, 
        an instance of a frozen dataclass, Fixed_locations
    params is accessed from other scripts by 
        from sp500_earn_price_pkg.config_paths import params
'''
import sys

from pathlib import Path
from dataclasses import dataclass
import json

# root for project, top-level proj directory
BASE_DIR = Path.cwd()
    
# location of variable environment settings
ENVIRONMENT = BASE_DIR / "environment.json"
if ENVIRONMENT.exists():
    with ENVIRONMENT.open('r') as f:
        ENVIRONMENT_DICT = json.load(f)


@dataclass(frozen= True, slots= True)
class Fixed_locations:
   
    # arch of previously used data files
    # arch to be specified by user in environment.json
    ARCHIVE_DIR = Path(ENVIRONMENT_DICT.get("archive_path", ""))
    if ARCHIVE_DIR == Path(""):
        print('\nERROR\n',
              'In environment.json, either:\n',
              '\tthe value for key "archive_path" is empty or\n',
              '\tthe key "archive_path" is missing.\n',
              'Provide the proper value for the key.\n')
        sys.exit()

    # source of new data, recorded in the output file
    # '.../sp500_earn_price_pkg/input_output/output_dir/record_dict.json'
    SP_SOURCE = Path(ENVIRONMENT_DICT.get('sp_source', None))
    #SP_SOURCE_XLSX = PATH(ENVIRONMENT_DICT.get('sp_source_xlsx', None))
    REAL_RATE_SOURCE = Path(ENVIRONMENT_DICT.get('real_rate_source', None))
    
    RECORD_DICT_TEMPLATE = \
        {'sources': {'s&p': ENVIRONMENT_DICT.get('sp_source', None),             
                     'tips': ENVIRONMENT_DICT.get('real_rate_source', None)},
        'latest_file': '',
        'prev_used_files': [],    # [str]
        'other_prev_files': []}   # [str]

    # partial templates for file names
    FILE_INPUT_WKBK_PREFIX = 'sp-'
    INPUT_SP_FILE_GLOB_STR = f'{FILE_INPUT_WKBK_PREFIX}*.xlsx'
    FILE_OUTPUT_WKBK_PREFIX = 'sp-eps'
    
## ====================================================================
## ========== Set paths and addr within proj's dir ====================
## ====================================================================
    INPUT_OUTPUT_DIR = BASE_DIR / "input_output"
    
    INPUT_DIR = INPUT_OUTPUT_DIR / "input_dir"
    # INPUT_DIR = ARCHIVE_DIR
    # to reinitialize all data in the project, using all archived history:
    #    put # before the statement for INPUT_DIR immed. above
    #    remove # before INPUT_DIR = ARCHIVE_DIR
    #    make sure 'DFII10.xlsx' is in ARCHIVE_DIR
    #    reverse these steps to return to normal operation
    
    # ensure this file is in INPUT_DIR (see above)
    INPUT_RR_FILE = 'DFII10.xlsx'
    INPUT_RR_ADDR = INPUT_DIR / INPUT_RR_FILE
    
    RECORD_DICT_DIR = INPUT_OUTPUT_DIR
    RECORD_DICT_FILE = "record_dict.json"
    RECORD_DICT_ADDR = RECORD_DICT_DIR / RECORD_DICT_FILE
    
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
    BACKUP_HIST_TEMP_ADDR = BACKUP_DIR / "temp_actuals.parquet"
    
    BACKUP_IND_FILE  = "backup_ind_df.parquet"
    BACKUP_IND_ADDR  = BACKUP_DIR / BACKUP_IND_FILE
    BACKUP_IND_TEMP_ADDR = BACKUP_DIR / "temp_ind.parquet"
    
    BACKUP_PROJ_FILE  = "backup_pe_estimates_df.parquet"
    BACKUP_PROJ_ADDR  = BACKUP_DIR / BACKUP_PROJ_FILE
    BACKUP_PROJ_TEMP_ADDR = BACKUP_DIR / "temp_proj.parquet"
    
    BACKUP_RECORD_DICT = "backup_record_dict.json"
    BACKUP_RECORD_DICT_ADDR = BACKUP_DIR / BACKUP_RECORD_DICT
    BACKUP_RECORD_TEMP_ADDR = BACKUP_DIR /  "temp_record.json"

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

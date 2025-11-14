# S&P500 earnings yield and 10-year TIPS rate
#### initiated:  2023 08
#### current version:  2025 12
### Uses current and historical data
- index of stock prices for the S&P500
- operating and reported earnings for the S&P500
- projections of operating and reported earnings
- interest rate on 10-year TIPS (constant maturity)
- operating margins for the S&P500
- earnings and prices for the S&P's industries

### update_data.py
- reads new data from .xlsx workbooks in input_dir/
- S&P data downloaded from S&P's "weekly" posts
- TIPS data downloaded from FRED database
- writes json and parquet files to output_dir/
- archives the workbooks from input_dir/

### display_data.py
- reads sp500_pe_df_actuals.parquet in output_dir/
- reads the files in output_dir/estimates/
- produces pdf documents in display_dir/
- presents quarterly data, 2018 through the present
    - page0: projected versus actual earnings
    - page1: future and historical price-earnings ratios
    - page2: margin and equity premium using trailing earnings
    - page3: equity premium using projected earnings

### display_ind_data.py
- reads sp500_ind_df.parquet in output_dir/
- produces pdf documents in display_dir/
- presents annual data, 2008 through the present
    - page4: distribution of industries' operating P/Es
    - page5: correlation heatmap for industries' operating P/Es
    - page6: distribution of industries' operating earnings

### sources
- https://www.spglobal.com/spdji/en/search/?query=index+earnings&activeTab=all
- https://fred.stlouisfed.org/series/DFII10/chart

### pyproject.toml
```
[project]
name = "sp500-earn-price-pkg"
version = "2.0.0"
description = "Compare S&P 500's earn-price ratio to 10-yr TIPS rate"
readme = "README.md"
authors = [
    { name = "RW Kopcke", email = "rwkopckel@yahoo.com" }
]
requires-python = ">=3.12.4"
dependencies = [
    "matplotlib>=3.10.3",
    "openpyxl>=3.1.5",
    "polars>=1.31.0",
    "pyarrow>=21.0.0",
    "scipy>=1.16.1",
    "seaborn>=0.13.2",
]

[project.scripts]
earn-price = "sp500_earn_price_pkg.entry:main"

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"
```

### project file structure

#### Dependencies:
```     
... /sp500_earn_price_pkg % uv tree
sp500-earn-price-pkg v2.0.0
├── matplotlib v3.10.6
│   ├── contourpy v1.3.3
│   │   └── numpy v2.3.3
│   ├── cycler v0.12.1
│   ├── fonttools v4.60.0
│   ├── kiwisolver v1.4.9
│   ├── numpy v2.3.3
│   ├── packaging v25.0
│   ├── pillow v11.3.0
│   ├── pyparsing v3.2.4
│   └── python-dateutil v2.9.0.post0
│       └── six v1.17.0
├── openpyxl v3.1.5
│   └── et-xmlfile v2.0.0
├── polars v1.33.1
├── pyarrow v21.0.0
├── scipy v1.16.2
│   └── numpy v2.3.3
└── seaborn v0.13.2
    ├── matplotlib v3.10.6 (*)
    ├── numpy v2.3.3
    └── pandas v2.3.2
        ├── numpy v2.3.3
        ├── python-dateutil v2.9.0.post0 (*)
        ├── pytz v2025.2
        └── tzdata v2025.2
(*) Package tree already displayed
```
<br>

#### Project File Structure

... /sp500_earn_price_pkg % tree
```
.
├── dist
│   ├── sp500_earn_price_pkg-1.0.0-py3-none-any.whl
│   └── sp500_earn_price_pkg-1.0.0.tar.gz
├── input_output
│   ├── backup_dir
│   │   ├── backup_ind_df.parquet
│   │   ├── backup_pe_df_actuals.parquet
│   │   ├── backup_pe_estimates_df.parquet
│   │   └── backup_record_dict.json
│   ├── display_dir
│   │   ├── eps_page0.pdf
│   │   ├── eps_page1.pdf
│   │   ├── eps_page2.pdf
│   │   ├── eps_page3.pdf
│   │   ├── eps_page4.pdf
│   │   ├── eps_page5.pdf
│   │   └── eps_page6.pdf
│   ├── input_dir
│   │   ├── DFII10.xlsx
│   │   └── sp-eps 2025-11-05.xlsx
│   ├── output_dir
│   │   ├── sp500_ind_df.parquet
│   │   ├── sp500_pe_df_actuals.parquet
│   │   └── sp500_pe_df_estimates.parquet
│   └── record_dict.json
├── pyproject.toml
├── README.md
├── src
│   └── sp500_earn_price_pkg
│       ├── __init__.py
│       ├── config
│       │   ├── config_paths.py
│       │   └── set_params.py
│       ├── entry.py
│       ├── helper_func_module
│       │   ├── __init__.py
│       │   ├── display_helper_func.py
│       │   └── helper_func.py
│       └── principal_scripts
│           ├── __init__.py
│           ├── code_segments
│           │   ├── __init__.py
│           │   ├── display_data
│           │   │   ├── __init__.py
│           │   │   ├── plot_func.py
│           │   │   └── read_data_for_display.py
│           │   ├── display_ind
│           │   │   ├── __init__.py
│           │   │   ├── plot_ind_func.py
│           │   │   └── read_ind_for_display.py
│           │   └── update_data
│           │       ├── __init__.py
│           │       ├── read_data.py
│           │       ├── update_record.py
│           │       └── write_data_to_files.py
│           ├── display_data.py
│           ├── display_ind_data.py
│           └── update_data.py
└── uv.lock

```
<br>
<br>

## Instructions
0. Set ARCHIVE_DIR

1. Put new .xlsx from S&P into input_dir/    (S&P's id: EPSEST)
    - https://www.spglobal.com/spdji/en/search/?query=index+earnings&activeTab=all
    - rename: sp-500-eps-est YYYY MM DD.xlsx
    
2. Put new data from FRED into input_dir/  (FRED's id: DFII10)
    - https://fred.stlouisfed.org/series/DFII10/chart
    - In DFII10.xlsx, add real interest rates for dates that match SP's new file dates

3. From sp500_earn_price_pkg top level: ```uv run earn-price```

    - action 0: update_data.py
        - reads files in input_dir/
        - moves input files to archive
        - writes the existing record_dict.json to backup_dir/
        - writes new record_dict to sp500-ep-project/record_dict.json
        - moves sp500_pe_df_actuals.parquet to backup_dir/
        - moves sp500_ind_df.parquet to backup_dir/
        - writes new sp500_pe_df_actuals.parquet to output_dir/
        - writes new sp500_ind_df.parquet to output_dir/
        - writes new sp500_pe_df_estimates.parquet to output_dir/

    - action 1: display_data.py
        - reads record_dict.json
        - reads sp500_pe_df_actuals.parquet file in output_dir/
        - reads sp-500-eps-est yyyy-mm-dd.parquet files in estimates/
        - writes pdf pages to display_dir/

    - action 2: display_ind_data.py
        - reads files in output_dir/
        - reads sp500_ind_df.parquet file in output_dir/
        - writes pdf pages to display_dir/
<br>
<br>

## Other Information
### sp500_earn_price_pkg/config/config_paths.py
-  Contains absolute address of ARCHIVE_DIR
    - user must specify location of ARCHIVE_DIR which contains input files after they have been read
-  Contains addresses for all other files relative to the address of the top-level project file
    - abs addr of top-level sp500_earn_price_pkg/ is the root for addrs of all folders and files (except the ARCHIVE_DIR)
- uses pathlib's Path()

### output_dir/
#### sp-500-eps-est YYYY MM DD.parquet
- polars dataframe with projected earnings
- from sp-500-eps-est YYYY MM DD.xlsx
- uses files with the latest date for each quarter
#### sp500_pe_df_actuals.parquet
- one polars dataframe for all historical data
- updated from new input data
### record_dict.json
- records all data files read and written
- records which files have been used
- maintains date of latest file read
- maintains list of quarters covered by data
<br>
<br>

#### To recreate/reinitialize output files from all archived history
1. see src/sp500_earn_price_pkg/config/config_paths.py
2. debug
    - ensure that updated DFII10.xlsx is in INPUT_DIR
    - move the most recent input file from ARCHIVE_DIR to INPUT_DIR
    - remove the name of the most recent input file from all keys in record_dict.json
3. reinitialize
    - ensure that DFII10.xlsx in INPUT_DIR has data for all quarters
    - in config_paths.py set INPUT_DIR = ARCHIVE_DIR
    - after reinitialization, reset INPUT_DIR 


#### Future improvements
- Tidy the logic/code in the scripts, incl separating functions in code
    - validate input files
    - check for new data
    - add 1-yr cm TIPS rate
    - add new page of equity premiums using 1-yr TIPS & projected E/P
- Convert the data structure from polars dataframes to duckdb
    - do not reload existing data from db
    - only insert new rows and tables
- Add chron feature to fetch new data from each week
    - fetch s&p .xlsx files with renaming
    - fetch corresponding data from Fred 

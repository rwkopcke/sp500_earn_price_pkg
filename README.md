# S&P500 earnings yield and 10-year TIPS rate
#### initiated:  2023 08
#### current version:  2026 01
### Uses current and historical data
- index of stock prices for the S&P500
- operating and reported earnings for the S&P500
- projections of operating and reported earnings
- interest rate on 10-year TIPS (constant maturity)
- operating margins for the S&P500
- earnings and prices for the S&P's industries
- earnings and prices for S&P 400, 600, and 1500 indexes

```
# navigate to the project's "outer folder" which contains 
#   pyproject.toml, uv.lock, src/ ...
#   see the Project File Tree below

cd /.../sp500-earn-price-pkg
uv run earn-price
```
### This CLI command then allows the user to run:

### update_data
- maintains files containing quarterly data
- observations are for the last available date in each quarter
- reads new data from .xlsx workbooks in input_dir/
- S&P data downloaded from S&P's "weekly" posts
- TIPS data downloaded from FRED database
- writes json and parquet files to output_dir/
- archives the workbooks from input_dir/

### display_sp500
- reads sp500_pe_df_actuals.parquet in output_dir/
- reads the files in output_dir/estimates/
- produces pdf documents in display_dir/
- presents quarterly data, 2018 through the present
    - page0: projected versus actual earnings
    - page1: future and historical price-earnings ratios
    - page2: margin and equity premium using trailing earnings
    - page3: equity premium using projected earnings

### display_sp500_ind
- reads sp500_ind_df.parquet in output_dir/
- produces pdf documents in display_dir/
- presents annual data, 2008 through the present
    - page4: distribution of industries' operating P/Es
    - page5: correlation heatmap for industries' operating P/Es
    - page6: distribution of industries' operating earnings

### display_sp_indexes
- TODO
- x

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

### Project Structure

#### Dependencies Tree:
```     
... /sp500-earn-price-pkg % uv tree
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

#### Project File Tree:

... /sp500-earn-price-pkg % tree
```
.
├├── environment.json
├── input_output
│   ├── backup_dir
│   │   └── temp_dir
│   ├── display_dir
│   │   ├── eps_page0.pdf
│   │   ├── eps_page1.pdf
│   │   ├── eps_page2.pdf
│   │   ├── eps_page3.pdf
│   │   ├── eps_page4.pdf
│   │   ├── eps_page5.pdf
│   │   └── eps_page6.pdf
│   ├── input_dir
│   │   └── DFII10.xlsx
|   |   |__ sp-*.xlsx
│   ├── output_dir
│   │   ├── sp500_ind_df.parquet
│   │   ├── sp500_pe_df_actuals.parquet
│   │   └── sp500_pe_df_estimates.parquet
│   └── record_dict.json
├── pyproject.toml
├── README.md
├── src
│   ├── config
│   │   ├── __init__.py
│   │   ├── config_paths.py
│   │   └── set_params.py
│   └── sp500_earn_price_pkg
│       ├── __init__.py
│       ├── display_sp_indexes
│       │   ├── __init__.py
│       │   └── sp_indexes_main.py
│       ├── display_sp500
│       │   ├── __init__.py
│       │   ├── plot_sp500.py
│       │   ├── read_sp500_for_display.py
│       │   └── sp500_main.py
│       ├── display_sp500_ind
│       │   ├── __init__.py
│       │   ├── ind_sp500_main.py
│       │   ├── plot_ind_sp500.py
│       │   └── read_ind_sp500.py
│       ├── entry.py
│       ├── helper_func
│       │   ├── __init__.py
│       │   ├── display_helper_func.py
│       │   └── helper_func.py
│       └── update_data
│           ├── __init__.py
│           ├── read_data.py
│           ├── update_main.py
│           ├── update_record.py
│           └── write_data_to_files.py
└── uv.lock

```
<br>
<br>

## Instructions
0. In .../sp500-earn-price-pkg/environment.json:
    - key "archive_path": **required value** "*absolute address for archive directory*"
    - key "sp_source": optional value "*web address for SP's EPSEST xlsx*"
    - key "real_rate_source": optional value "*web address for FRED's DFII10*"
    - key "rate_of_growth_of_sp_index": optional decimal value, defaults to 0.05, example annual rate of growth of stock prices during quarters of projected E, used only for page 1.

1. Put new .xlsx from S&P into input_dir/    (S&P's id: EPSEST)
    - https://www.spglobal.com/spdji/en/search/?query=index+earnings&activeTab=all
    - first three characters of sp input file must be "sp-"
    - last four characters of sp input file must be ".xlsx" (see tree above)
    
2. Put new data from FRED into input_dir/DFII10.xlsx  (FRED's id: DFII10)
    - https://fred.stlouisfed.org/series/DFII10/chart
    - In DFII10.xlsx, add real interest rates for dates that match SP's new file dates
    - Name of FRED file must be "DFII10.xlsx"  (see tree above)
    - DFII10.xlsx must have 
        - quarter-end 10-year TIPS rates for full period of sp files
        - TIPS 10-year rate for the date of most recent sp file

3. ```uv run earn-price```
    - action 0: update_data
        - reads files in input_dir/
        - moves input files to archive
        - writes the existing record_dict.json to backup_dir/
        - writes new record_dict to sp500-ep-project/record_dict.json
        - moves sp500_pe_df_actuals.parquet to backup_dir/
        - moves sp500_ind_df.parquet to backup_dir/
        - writes new sp500_pe_df_actuals.parquet to output_dir/
        - writes new sp500_ind_df.parquet to output_dir/
        - writes new sp500_pe_df_estimates.parquet to output_dir/

    - action 1: display_sp500
        - reads record_dict.json
        - reads sp500_pe_df_actuals.parquet file in output_dir/
        - reads sp-500-eps-est yyyy-mm-dd.parquet files in estimates/
        - writes pdf pages to display_dir/

    - action 2: display_sp500_ind
        - reads files in output_dir/
        - reads sp500_ind_df.parquet file in output_dir/
        - writes pdf pages to display_dir/
    
    - action 3: display_sp_indexes

4. uncaught runtime exceptions
    - are handled by custom hooks that override ```sys.excepthook```
    - each action has its own excepthook defined in its *_main.py 
    - uncaught exceptions for update_data restore parquet file and record_dict before processing quits
<br>
<br>

## Other Information
### sp500-earn-price-pkg/config/config_paths.py
-  Reads absolute address of ARCHIVE_DIR from ```/sp500_earn_price_pkg/environment.json```
    - user must specify location of ARCHIVE_DIR to store input files after they have been read
-  Contains addresses for all other files relative to the address of the top-level project file
    - abs addr of top-level sp500_earn_price_pkg/ is the root for addrs of all folders and files (except the ARCHIVE_DIR)

### sp500-earn-price-pkg/input_output/
#### output_dir/sp500_pe_df_estimates.parquet
- polars dataframe with projected earnings
- uses estimates for the latest date for each quarter
#### output_dir/sp500_pe_df_actuals.parquet
- polars dataframe for all historical data
#### output_dir/sp500_ind_df.parquet
- polars dataframe with annual earnings and pe ratios for 11 ind segments
- data for the sp400, sp500, sp600, and sp1500
### record_dict.json
- records all data files read and written
- records which files have been used
- maintains date of latest file read
<br>
<br>

#### To recreate/reinitialize output files from all archived history
1. see ```src/sp500_earn_price_pkg/config/config_paths.py```
2. debug
    - ensure that updated DFII10.xlsx is in INPUT_DIR
    - move input files to be reread from ARCHIVE_DIR to INPUT_DIR
    - remove the input files to read from the current record_dict.json
        - "latest_file"
        - "prev_used_files"
        - "other_prev_files"
3. reinitialize
    - ensure that DFII10.xlsx in INPUT_DIR has data for all quarters
    - in config_paths.py set INPUT_DIR = ARCHIVE_DIR
    - after reinitialization, reset INPUT_DIR 


#### Future improvements
- Add comparisons of 500 to 400, 600, 1500
    - Shares of total earnings
    - EP ratios, both for index and for industries within
    - Correlations
    - Industry (using op_e) mix, comparative bar charts
- Tidy the logic/code in the scripts
- Improve docstrings (incremental)
- Add 1-yr cm TIPS rate ???? (low priority, if at all)
    - add new page of equity premiums using 1-yr TIPS & projected E/P
- Convert the data structure from polars dataframes to duckdb or ... 

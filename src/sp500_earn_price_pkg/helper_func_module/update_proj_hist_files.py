import polars as pl


def update(env):
    '''
        Archive all files in new_files_set
        Read all files in files_to_read_set
        Updates proj_dict
        
        Proj_dict contains projections
            key: yr_qtr, in which the proj was made
            val: df containing the projs for future dates
        The projs for each yr_qtr are the proj for the 
            latest date in the quarter (.xlsx workbooks)
            
        Return proj_dict, contains keys for all yr_qtrs
            to date (data begins in 2017)
    '''
    

## ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
## ++++ Create proj_dict from proj_hist_df stored in parquet file +++++
## ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    if env().BACKUP_PROJ_ADDR.exists():
        with env().BACKUP_PROJ_ADDR.open('rb') as f:
            proj_hist_df = pl.read_parquet(f)
        with env().BACKUP_PROJ_ADDR.open('wb') as f:
            proj_hist_df.write_parquet(f)
        proj_dict = proj_hist_df.to_dict(as_series= False)
    else:
        proj_dict = dict()
    
    return proj_dict
        
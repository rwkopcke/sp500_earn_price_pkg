import sp500_earn_price_pkg.config.config_paths as config
from sp500_earn_price_pkg.helper_func_module \
    import helper_func as hp

def write(actual_df, ind_df):
         
## +++++ write history file +++++++++++++++++++++++++++++++++++++++++++
    # move any existing hist file in output_dir to backup
    if config.Fixed_locations().OUTPUT_HIST_ADDR.exists():
        config.Fixed_locations().OUTPUT_HIST_ADDR.rename(config.Fixed_locations().BACKUP_HIST_ADDR)
        hp.message([
            f'Moved history file from: \n{config.Fixed_locations().OUTPUT_HIST_ADDR}',
            f'to: \n{config.Fixed_locations().BACKUP_HIST_ADDR}'
        ])
    else:
        hp.message([
            f'Found no history file at: \n{config.Fixed_locations().OUTPUT_HIST_ADDR}',
            f'Wrote no history file to: \n{config.Fixed_locations().BACKUP_HIST_ADDR}'
        ])
        
    # write actual_df, the historical data, into the output_dir
    with config.Fixed_locations().OUTPUT_HIST_ADDR.open('w') as f:
        actual_df.write_parquet(f)
    hp.message([
            f'Wrote history file to: \n{config.Fixed_locations().OUTPUT_HIST_ADDR}'
    ])
    
## +++++ write industry file ++++++++++++++++++++++++++++++++++++++++++
    # move any existing industry file in output_dir to backup
    if config.Fixed_locations().OUTPUT_IND_ADDR.exists():
        config.Fixed_locations().OUTPUT_IND_ADDR.rename(config.Fixed_locations().BACKUP_IND_ADDR)
        hp.message([
            f'Moved industry file from: \n{config.Fixed_locations().OUTPUT_IND_ADDR}',
            f'to: \n{config.Fixed_locations().BACKUP_IND_ADDR}'
        ])
    else:
        hp.message([
            f'Found no industry file at: \n{config.Fixed_locations().OUTPUT_IND_ADDR}',
            f'Wrote no industry file to: \n{config.Fixed_locations().BACKUP_IND_ADDR}'
        ])
        
    # write ind_df, the industry data, into the output_dir
    with config.Fixed_locations().OUTPUT_IND_ADDR.open('w') as f:
        ind_df.write_parquet(f)
    hp.message([
        f'Wrote industry file to: \n{config.Fixed_locations().OUTPUT_IND_ADDR}'
    ])
    
    return

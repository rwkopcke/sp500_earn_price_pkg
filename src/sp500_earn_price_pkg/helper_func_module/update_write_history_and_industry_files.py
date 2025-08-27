import sp500_earn_price_pkg.config.config_paths as config

def write(actual_df, ind_df):
         
## +++++ write history file +++++++++++++++++++++++++++++++++++++++++++
    # move any existing hist file in output_dir to backup
    if config.Fixed_locations().OUTPUT_HIST_ADDR.exists():
        config.Fixed_locations().OUTPUT_HIST_ADDR.rename(config.Fixed_locations().BACKUP_HIST_ADDR)
        print('\n============================================')
        print(f'Moved history file from: \n{config.Fixed_locations().OUTPUT_HIST_ADDR}')
        print(f'to: \n{config.Fixed_locations().BACKUP_HIST_ADDR}')
        print('============================================\n')
    else:
        print('\n============================================')
        print(f'Found no history file at: \n{config.Fixed_locations().OUTPUT_HIST_ADDR}')
        print(f'Wrote no history file to: \n{config.Fixed_locations().BACKUP_HIST_ADDR}')
        print('============================================\n')
        
    # write actual_df, the historical data, into the output_dir
    with config.Fixed_locations().OUTPUT_HIST_ADDR.open('w') as f:
        actual_df.write_parquet(f)
    print('\n============================================')
    print(f'Wrote history file to: \n{config.Fixed_locations().OUTPUT_HIST_ADDR}')
    print('============================================\n')
    
## +++++ write industry file ++++++++++++++++++++++++++++++++++++++++++
    # move any existing industry file in output_dir to backup
    if config.Fixed_locations().OUTPUT_IND_ADDR.exists():
        config.Fixed_locations().OUTPUT_IND_ADDR.rename(config.Fixed_locations().BACKUP_IND_ADDR)
        print('\n============================================')
        print(f'Moved industry file from: \n{config.Fixed_locations().OUTPUT_IND_ADDR}')
        print(f'to: \n{config.Fixed_locations().BACKUP_IND_ADDR}')
        print('============================================\n')
    else:
        print('\n============================================')
        print(f'Found no industry file at: \n{config.Fixed_locations().OUTPUT_IND_ADDR}')
        print(f'Wrote no industry file to: \n{config.Fixed_locations().BACKUP_IND_ADDR}')
        print('============================================\n')
        
    # write ind_df, the industry data, into the output_dir
    with config.Fixed_locations().OUTPUT_IND_ADDR.open('w') as f:
        ind_df.write_parquet(f)
    print('\n============================================')
    print(f'Wrote industry file to: \n{config.Fixed_locations().OUTPUT_IND_ADDR}')
    print('============================================\n')
    
    return

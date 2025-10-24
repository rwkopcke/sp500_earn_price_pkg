import json

import sp500_earn_price_pkg.config.config_paths as config
from sp500_earn_price_pkg.helper_func_module \
    import helper_func as hp


def write(record_dict):
    with open(config.Fixed_locations().RECORD_DICT_ADDR, 'w') as f:
        json.dump(record_dict, f, indent= 4)

    record_dict_prev_used_files = \
        record_dict['prev_used_files'][:4]
    hp.message([
        'Saved record_dict to file',
        f'{config.Fixed_locations().RECORD_DICT_ADDR}',
        f'\nlatest_used_file: {record_dict['latest_used_file']}\n',
        f'prev_files: \n{record_dict['prev_files'][:4]}\n',
        f"record_dict['prev_used_files']: \n{record_dict_prev_used_files}\n"
    ])
    
    return

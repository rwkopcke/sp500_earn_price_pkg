import sys
from types import MappingProxyType

from sp500_earn_price_pkg.principal_scripts import (
        display_other_sp_indexes,
        update_data, 
        display_data,
        display_ind_data
        )


def main():
    '''
        Calls the main scripts that produce this
        project's data and displays
    '''
    
    def not_a_valid_key():
        print(f'\n{action} is not a valid key')

    action_dict = MappingProxyType({
        "0": 'Update data from recent S&P and FRED workbooks',
        "1": 'Generate Displays for the S&P500 Index',
        "2": 'Generate Displays for the S&P500 Industries',
        "3": 'Generate Displays for the 4 S&P Indexes'})
    
    function_dict = MappingProxyType({
        "0": update_data.update,
        "1": display_data.display,
        "2": display_ind_data.display_ind,
        "3": display_other_sp_indexes.display_spdexes})

    # request actions from user (cli)
    while True:
        
        print('\n' * 2)
        for key in action_dict.keys():
            print(f'{key}: {action_dict[key]}')

        action = input(
            '\nEnter the key for the intended action: ')
        
        function_dict.get(action, not_a_valid_key)()
                
        choice = input(
            '\nTo take another action, type T; otherwise, type F: ')
        
        if choice not in ['T', 't', 'True', 'Y', 'y', 'yes']:
            print(f'{choice} End process.')
            sys.exit()
            
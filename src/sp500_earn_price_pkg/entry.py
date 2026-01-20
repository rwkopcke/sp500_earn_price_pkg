'''
    CLI: Entry point for package
        uv run earn-price
    Allows user to select an action
        explained in main() below
'''

import sys
from types import MappingProxyType

from sp500_earn_price_pkg.principal_scripts import (
        display_other_sp_indexes,
        update_data, 
        display_data,
        display_ind_data
        )
from sp500_earn_price_pkg.principal_scripts\
    .code_segments.update_data \
        import write_data_to_files as write
    

# set custom error handler
def my_excepthook(exctype, value, traceback):
    '''
        Before reporting the exception in standard format,
        Rename (restore) any temp files back to original names
    '''
    write.restore_data_stop_update(location= "excepthook",
                                   exit= False)
    if exctype == KeyboardInterrupt:
        print("Process interrupted by keyboard command.")
    else:
        sys.__excepthook__(exctype, value, traceback)
        
sys.excepthook = my_excepthook


def main():
    '''
        Calls the main scripts that produce this
        project's data and displays
    '''
    # user selects an action from this menu ...
    action_dict = MappingProxyType({
        "0": 'Update data from recent S&P and FRED workbooks',
        "1": 'Generate Displays for the S&P500 Index',
        "2": 'Generate Displays for the S&P500 Industries',
        "3": 'Generate Displays for the 4 S&P Indexes'})
    
    # ... to launch the corresponding function in this map ...
    function_dict = MappingProxyType({
        "0": update_data.update,
        "1": display_data.display,
        "2": display_ind_data.display_ind,
        "3": display_other_sp_indexes.display_spdexes})
    
    # ... response when user's selection is not valid
    def not_a_valid_key():
        print(f'\n{action} is not a valid key')
        

    # request actions from user (cli)
    while True:
        
        print('\n' * 2)
        for key in action_dict.keys():
            print(f'{key}: {action_dict[key]}')

        action = input(
            '\nEnter the key for the intended action: ')
        
        function_dict.get(action, not_a_valid_key)()
                
        choice = input(
            '\nFor another action, enter "true"; otherwise, enter any other character: '
            )
        
        if choice not in ['true', '"true"']:
            print(f'{choice} End process.')
            sys.exit()
            
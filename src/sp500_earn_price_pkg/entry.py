'''
    CLI entry point for package
        uv run earn-price
    Allows user to select an action
        explained in main() below
'''

from types import MappingProxyType

from sp500_earn_price_pkg.update_data \
    import update_main
from sp500_earn_price_pkg.display_sp500 \
    import sp500_main
from sp500_earn_price_pkg.display_sp500_ind \
    import ind_sp500_main
from sp500_earn_price_pkg.display_sp_indexes \
    import sp_indexes_main


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
        "0": update_main.update,
        "1": sp500_main.display,
        "2": ind_sp500_main.display_ind,
        "3": sp_indexes_main.display_indexes})
    
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
            break
            
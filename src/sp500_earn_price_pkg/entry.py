def main():
    '''
        Calls the three main scripts that produce this
        project's data and displays
    '''
    
    # import main scripts
    from sp500_earn_price_pkg.principal_scripts import (
        update_data, 
        display_data,
        display_ind_data
    )

    action_dict = {
        "0": 'Update data from recent S&P and FRED workbooks',
        "1": 'Generate Displays for the S&P500 Index',
        "2": 'Generate Displays for the S&P500 Industries'
    }

    # request actions from user (cli)
    while True:
        
        print('\n' * 2)
        for key in action_dict.keys():
            print(f'{key}: {action_dict[key]}')

        action = input(
            '\nEnter the key shown above for the intended action: ')
        
        match action:
            case "0":
                update_data.update()
            case "1":
                display_data.display()
            case "2":
                display_ind_data.display_ind()
            case _:
                print(f'{action} is not a valid key')
                
        choice = input(
            '\nTo take another action, type T; otherwise, type F: ')
        
        # quit if choice does not conform
        if choice not in ['T', 't', 'True', 'Y', 'y', 'yes']:
            print(f'{choice} End process.')
            quit()
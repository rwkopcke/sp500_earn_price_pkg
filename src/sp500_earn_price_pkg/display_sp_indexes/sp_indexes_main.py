import sys

import matplotlib.pyplot as plt

'''
# set custom error handler
def indexes_excepthook(exctype, value, traceback):
    ''
        Before reporting the exception in standard format,
        Rename (restore) any temp files back to original names
    ''
    write.restore_data_stop_update(location= "excepthook",
                                   exit= False)
    if exctype == KeyboardInterrupt:
        print("Process interrupted by keyboard command.")
    else:
        sys.__excepthook__(exctype, value, traceback)
'''

def display_indexes():
    
    #sys.excepthook = indexes_excepthook
    
    print('code to come')
    pass


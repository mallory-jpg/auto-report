"""Create key, value pairs with vin_number as key"""

import sys

#input comes from STDIN
for line in sys.stdin:
    # split by commas
    str_values = line.split(',')
    # incident type
    type_val = str_values[1]
    # vin number = key
    vim_num_key = str_values[2]
    # car make
    make_val = str_values[3]
    # car year
    year_val = str_values[5]
    # value to vin number key
    value = (type_val, make_val, year_val)
    print(f'{vim_num_key}\t{value}')

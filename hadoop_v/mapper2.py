"""
Create composite keys of make + year and count incidences.
:input reducer1.py: output from first MapReduce operation via STDIN
:return new_key: composite make+year key and incidence count 
"""

import sys


#input comes from reducer1 through STDIN
for line in sys.stdin:

    str_values = line.split('\t')
    vin = str_values[0]
    #value tuple = (type, make, year)
    value = eval(str_values[1])
    make_val = value[1]
    year_val = value[2]
    # composite key
    new_key = f'{make_val}{year_val}'
    # output composite key and count (1)
    print(f'{new_key}\t1')

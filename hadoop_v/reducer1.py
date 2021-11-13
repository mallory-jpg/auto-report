"""Define group-level master information"""

import sys

# dictionary for incident type I
i_dict = {}  # Key = vin_number, value = (type, make, year)
current_key = None
value_list = []


def reset():
    """Reset master info for each new group"""
    
    # define global variables
    global i_dict
    global current_key
    global value_list

    i_dict = {}
    current_key = None
    value_list = []


def flush():
    """
    Runs at the end of each new group to show key, value pairs.
    :param: None
    :return current_key: vin_number
    :return value: (type, make, year) tuple
    """
    
    # define global variables
    global i_dict
    global current_key
    global value_list
    
    # write  output
    for value in value_list:
        type_val = value[0]
        # using only incident_types of accidents
        if type_val == 'A':
            # Get make, year from the dictionary using vin number key
            make_val = i_dict[current_key][1]
            year_val = i_dict[current_key][2]
            value = (type_val, make_val, year_val)
            print(f'{current_key}\t{value}')
        else:
            # skip non-accident incident records
            continue


# input from stdin is sorted key, value pairs
for line in sys.stdin:
    # parse the input we got from mapper 
    str_values = line.split('\t')
    # update master info
    key = str_values[0]
    # convert string to tuple
    value = eval(str_values[1])
    # Update dictionary
    type_val = value[0]
    if type_val == 'I':
        # add (key, value) to dictionary
        i_dict[key] = value
    # look for key changes
    if current_key != key:
        if current_key != None:
            # Write result to stdout
            flush()
        reset()
    # update more master info after the key change handling
    value_list.append(value)
    current_key = key

# do not forget to output the last group if needed!
flush()

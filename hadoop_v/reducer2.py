"""
Reducer 
:input mapper2.py: output of second mapping operation, composite key (make+year) with value of one for each incidence
:return: total count of accidents for each make+year key
"""


import sys


current_key = None
value_list = []


def reset():
    """Reset master information for each new group"""

    global current_key
    global value_list
    current_key = None
    value_list = []


# Run for end of every group
def flush():
    """
    Write updated output and run at the end of each new group
    :params: None
    :return current_key: 
    :return _sum:
    """
    global current_key
    global value_list
    # write the output
    _sum = 0

    for value in value_list:
        # each value is a tuple with format (type, make, year)
        count_val = value
        _sum += count_val
    print(f'{current_key}\t{_sum}')



for line in sys.stdin:
    # parse mapper input
    str_vals = line.split('\t')
    key = str_vals[0]
    value = int(str_vals[1])

    # detect key changes
    if current_key != key:
        if current_key != None:
            #Write result to stdout
            flush()
        reset()

    # update more master info
    value_list.append(value)
    current_key = key

# do not forget to output the last group if needed!
flush()

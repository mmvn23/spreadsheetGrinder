import glb_var.others
import datetime

def convert_string_to_bool(string_var):
    string_var = string_var.lower().replace(" ", "")
    if string_var == glb_var.others.true:
        bool_var = True
    elif string_var == glb_var.others.false:
        bool_var = False
    else:
        print('ERROR')
        bool_var='ERROR'

    return bool_var


def convert_string_to_timestamp(any_date, any_format):
    any_date = any_date[:-7]
    any_date = datetime.datetime.strptime(any_date, any_format)
    return any_date

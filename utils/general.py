import numpy as np
import pandas as pd
import copy
import variables.general
import variables.var_column as clmn
# import variables.setup_column as stp_clmn
import variables.type as tp
# import variables.setup.file as stp
import variables.general as var_gen
import datetime
import os


def get_value_from_dataframe(input_dataframe, target_column_list, column_list_to_filter, value_list_to_filter,
                             return_dataframe=False, column_to_return=0, return_list=False, reset_index=False):
    any_dataframe = copy.deepcopy(input_dataframe)

    if reset_index:
        any_dataframe.reset_index(inplace=True)

    # original_index_list = any_dataframe.index.names
    cond = ~any_dataframe.index.isnull()  #True condition

    if type(target_column_list) != list:
        target_column_list = [target_column_list]

    reset_index = set(any_dataframe.index).isdisjoint(set(target_column_list + column_list_to_filter))

    if reset_index:
        any_dataframe.reset_index(inplace=True)

    for ii in range(0, len(column_list_to_filter)):
        new_cond = any_dataframe[column_list_to_filter[ii]] == value_list_to_filter[ii]
        cond = cond & new_cond

    any_dataframe = any_dataframe[cond]

    if return_dataframe:
        # value = any_dataframe[original_index_list + target_column_list]
        value = any_dataframe[target_column_list]
    elif return_list:
        value = list(any_dataframe[target_column_list[column_to_return]])
    else:
        value = list(any_dataframe[target_column_list[column_to_return]])[0]

    return value


def get_filepath(root, folder, file, any_format):
    any_filepath = root + '/' + folder + '/' + file + '.' + any_format
    any_filepath = treat_filepath(any_filepath)
    return any_filepath


def treat_filepath(original_filepath):
    filepath = copy.deepcopy(original_filepath)
    filepath = assign_type(filepath, desired_type=tp.my_string)
    filepath = filepath.replace(chr(92), '/').replace('//', '/').replace('..', '.')
    return filepath


def assign_type_to_dict(input_dict, desired_type_list, date_parser=variables.general.date_parser_to_save):
    any_dict = copy.deepcopy(input_dict)

    dict_keys_list = any_dict.keys()

    if len(dict_keys_list) == len(desired_type_list):
        ii = 0
        for any_dict_key in dict_keys_list:
            any_dict[any_dict_key] = assign_type(any_dict[any_dict_key], desired_type_list[ii], date_parser)
            ii = ii + 1
    else:
        raise('Mismatch between keys and desired types')

    return any_dict


def assign_type_for_list(value_list, desired_type, date_parser='%m/%d/%Y'):
    new_value_list = []
    for any_value in value_list:
        new_value = assign_type(any_value, desired_type, date_parser)
        new_value_list = new_value_list + [new_value]
        
    return new_value_list


def assign_type(input_value, desired_type, date_parser='%m/%d/%Y'):
    if input_value != variables.general.not_applicable:
        any_value = copy.deepcopy(input_value)
        if desired_type == tp.my_int:
            any_value = int(any_value)
        elif desired_type == tp.my_float:
            any_value = float(any_value)
        elif desired_type == tp.my_string:
            any_value = str(any_value)
        elif desired_type == tp.my_bool:
            any_value = bool(any_value)
        elif desired_type == tp.my_date:
            any_value = parse_date_as_timestamp(any_value, date_parser)
            # pd.Timestamp(year=mytimestamp.year, month=mytimestamp.month, day=mytimestamp.day)
        else:
            raise('ERROR: type not implemented')
    else:
        any_value = input_value
    return any_value


# def parse_date_as_timestamp(original_date, date_parser_list):
def parse_date_as_timestamp(original_date, date_parser_list):
    successful_parsing = False

    for any_date_parser in date_parser_list:
        any_date_parsed, successful_parsing_to_append = parse_date_as_timestamp_per_data_parser(original_date,
                                                                                                any_date_parser)

        if successful_parsing_to_append:
            any_date = any_date_parsed
        successful_parsing = successful_parsing | successful_parsing_to_append

    if not successful_parsing:
        any_date = pd.NA

    return any_date


def parse_date_as_timestamp_per_data_parser(original_date, date_parser):
    successful_parsing = False
    any_date = ''

    try:
        if type(original_date) == str:
            any_date = datetime.datetime.strptime(original_date, date_parser)
        else:
            any_date = original_date
        any_date = pd.Timestamp(year=any_date.year, month=any_date.month, day=any_date.day)
        successful_parsing = True
    except ValueError:
        pass

    return any_date, successful_parsing


def convert_timestamp_to_str(mytimestamp, date_parser):
    # any_str = datetime.datetime.fromtimestamp(mytimestamp).strftime(date_parser)
    try:
        any_str = mytimestamp.strftime(date_parser)
    except:
        any_str = str(mytimestamp)

    return any_str


def get_date_from_month_and_period(month_day, period, fiscal_year):
    if pd.isnull(month_day):
        date = get_date_from_period_fiscal_year(period, fiscal_year)
    elif pd.isnull(period):
        date = get_date_from_month_fiscal_year(month_day, fiscal_year)
    else:
        date = month_day
    return date


def get_date_from_period_fiscal_year(period, fiscal_year):

    if period <= 6:
        month = period + 6
        year = 2000 + fiscal_year - 1
    else:
        month = period - 6
        year = 2000 + fiscal_year + 1

    return pd.Timestamp(freq='M', year=int(year), month=int(month), day=1)


def get_date_from_month_fiscal_year(date_month, fiscal_year):
    month = date_month.month
    day = date_month.day
    if month > 6:
        year = 2000 + fiscal_year - 1
    else:
        year = 2000 + fiscal_year + 1

    return pd.Timestamp(freq='M', year=int(year), month=int(month), day=int(day))


def convert_str_to_timestamp(column_str, date_parser):
    any_date = datetime.datetime.strptime(column_str, date_parser)
    any_date = pd.Timestamp(freq='M', year=any_date.year, month=any_date.month, day=any_date.day)
    return any_date


def get_now_timestamp():
    any_timestamp = datetime.datetime.now()
    any_timestamp = pd.Timestamp(freq='M', year=any_timestamp.year, month=any_timestamp.month,
                                 day=any_timestamp.day)
    return any_timestamp


def clean_string(any_value):
    any_value.replace(' ', '').lower()
    return any_value


def substitute_term_from_string(original_value, target_list, df_substitution=pd.DataFrame(), target_substitution_clmn='',
                                value_to_substitute_clmn=''):
    try:
        any_value = original_value.lower()

        deletion = len(df_substitution.index) == 0
        translation = not deletion

        ii = 0
        for any_term in target_list:

            if deletion:
                any_value = any_value.replace(any_term, '')

            if translation:
                substitution_value = get_value_from_dataframe(input_dataframe=df_substitution,
                                                              target_column_list=target_substitution_clmn,
                                                              column_list_to_filter=[value_to_substitute_clmn],
                                                              value_list_to_filter=[any_term])
                any_value = any_value.replace(any_term, substitution_value)

            ii = ii + 1
    except:
        any_value = original_value
        print(original_value, 'EXCEPT ACTIVATED - substitute string')

    return any_value


def trim_string(original_value):
    try:
        any_value = original_value.strip()
    except:
        any_value = original_value
        print(original_value, 'EXCEPT ACTIVATED - trim string')

    return any_value


def lower_string(original_value):
    try:
        any_value = original_value.lower()
    except:
        any_value = original_value
        print(original_value, 'EXCEPT ACTIVATED - lower string')
    return any_value


def trim_and_lower_string(original_value):
    any_value = trim_string(original_value)
    any_value = lower_string(any_value)
    return any_value


def get_multiplier_from_mtx_conversion(any_mtx_conversion, original, new, part_number):
    error_general = False
    error_specific = False

    if original == new:
        value = 1
    else:
        value, error_general = get_multiplier_general(any_mtx_conversion, original, new)
        if error_general:
            value, error_specific = get_multiplier_specific(any_mtx_conversion, original, new, part_number)

    error_combined = error_general | error_specific

    if error_combined:
        value = pd.NA

    return value


def get_multiplier_general(any_mtx_conversion, original, new):
    error = False

    try:
        value = get_value_from_dataframe(input_dataframe=any_mtx_conversion.dataframe,
                                         target_column_list=clmn.multiplier,
                                         column_list_to_filter=[clmn.original, clmn.new, clmn.strategy],
                                         value_list_to_filter=[original, new, var_gen.uom_general],
                                         reset_index=True)
    except:
        value = ''
        error =True
    finally:
        return value, error


def get_multiplier_specific(any_mtx_conversion, original, new, part_number):
    error = False

    try:
        value = get_value_from_dataframe(input_dataframe=any_mtx_conversion.dataframe,
                                         target_column_list=clmn.multiplier,
                                         column_list_to_filter=[clmn.original, clmn.new, clmn.strategy, clmn.part_number_code],
                                         value_list_to_filter=[original, new, var_gen.uom_specific, part_number],
                                         reset_index=True)
    except:
        value = ''
        error =True
    finally:
        return value, error


def remove_file_name_from_filepath(filepath, sub_str):
    filepath_list = filepath.split(sub_str)
    filepath_list = filepath_list[:-1]
    new_filepath = ''
    ii = 0

    for any_element in filepath_list:
        if ii > 0:
            new_filepath = new_filepath + sub_str + any_element
        else:
            new_filepath = new_filepath + any_element
        ii = ii + 1

    return new_filepath


def get_file_list_from_directory(filepath):
    return os.listdir(filepath)
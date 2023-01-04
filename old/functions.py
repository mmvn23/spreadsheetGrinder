import pandas as pd
import numpy as np

#############################                1) DATA CLEANING                     ######################################
import variables.setup.column


def import_data(dataframe, dataframe_clmn_lt, flnm_local_file, local_file_clmn_expect_lt, local_file_clmn_conversion,
                description_clmn, date_clmn, value_original_clmn, currency_clmn, origin_clmn, local_file_str,
                local_file_currency, local_file_multiplier, initial_date, type, sht_local_file=[],
                local_file_skiprows=0, credit_clmn=[], debit_clmn=[]):
    if type == 'csv':
        # a)load files
        df_local_file = pd.read_csv(flnm_local_file, index_col=False, encoding='ISO-8859-1')
    elif type == 'xlsx':
        df_local_file = pd.read_excel(flnm_local_file, sheet_name=sht_local_file)
    elif type == 'csv skiprows':
        df_local_file = pd.read_csv(flnm_local_file, index_col=False, skiprows=local_file_skiprows)
    elif type == 'csv pnncl':
        df_local_file = pd.read_csv(flnm_local_file, index_col=False)
    elif type == 'csv snvst':
        df_local_file = pd.read_csv(flnm_local_file, index_col=False, encoding='ISO-8859-1')

    # c) Drop extra columns
    df_local_file = df_local_file.filter(items=local_file_clmn_expect_lt, axis=1)

    # e) Change column names
    df_local_file.rename(columns=local_file_clmn_conversion, inplace=True)

    if type == 'csv snvst':
        df_local_file[date_clmn] = df_local_file.apply(lambda x: pd.to_datetime(x[date_clmn], dayfirst=True), axis=1)
    else:
        df_local_file[date_clmn] = df_local_file.apply(lambda x: pd.Timestamp(x[date_clmn]), axis=1)

    df_local_file = df_local_file[df_local_file[date_clmn] >= initial_date]

    # f) Match raw file to df_cashmov
    df_to_append = pd.DataFrame(columns=dataframe_clmn_lt)
    df_to_append[description_clmn] = df_local_file[description_clmn]

    if type == 'csv pnncl':
        df_local_file[credit_clmn] = df_local_file[credit_clmn].fillna(0)
        df_local_file[debit_clmn] = df_local_file[debit_clmn].fillna(0)
        df_to_append[value_original_clmn] = local_file_multiplier * df_local_file[credit_clmn] - \
                                                    local_file_multiplier * df_local_file[debit_clmn]
    elif type == 'csv snvst':
        df_to_append[value_original_clmn] = df_local_file[value_original_clmn].astype(str).apply(lambda x: x.replace(',', '.'))
        df_to_append[value_original_clmn].astype(float)
    else:
        df_to_append[value_original_clmn] = local_file_multiplier * df_local_file[value_original_clmn]

    if local_file_currency =='N/A':
        df_to_append[currency_clmn] = df_local_file[currency_clmn]
    else:
        df_to_append[currency_clmn] = local_file_currency

    df_to_append[date_clmn] = df_local_file[date_clmn]
    df_to_append[origin_clmn] = local_file_str

    df_to_append.dropna(inplace=True, how='all',
                                subset=[description_clmn, date_clmn, value_original_clmn, currency_clmn, origin_clmn])

    # g) Append dataframe
    dataframe = dataframe.append(df_to_append, ignore_index=True)

    return dataframe


def assign_budgets(dataframe, df_manual_bgt, df_auto_bgt, budget_clmn, date_clmn, manual_bgt_clmn, description_clmn,
                   contain_clmn, auto_bgt_clmn, origin_clmn, initial_date_clmn, end_date_clmn):

    # Manual budgeting
    dataframe = dataframe.merge(df_manual_bgt, how='left', left_index=True, right_index=True)
    dataframe[budget_clmn] = dataframe.apply(
        lambda row: pd.NA if pd.isna(row[manual_bgt_clmn]) else row[manual_bgt_clmn],
        axis=1)

    for kk in variables.setup.column.index:

        condition_0 = dataframe[description_clmn].str.contains(df_auto_bgt.loc[kk, contain_clmn])
        condition_1 = dataframe[budget_clmn].isnull()

        condition_2a = dataframe[origin_clmn].notnull()

        if not df_auto_bgt.loc[kk, origin_clmn] == np.nan:
            print(df_auto_bgt.loc[kk, origin_clmn])
            condition_2b = dataframe[origin_clmn].str.contains(df_auto_bgt.loc[kk, origin_clmn])
            condition_2 = condition_2a & condition_2b
        else:
            condition_2 = condition_2a

        if df_auto_bgt.loc[kk, initial_date_clmn].notnull():
            condition_3 = dataframe[date_clmn] >= df_auto_bgt.loc[kk, initial_date_clmn]
            condition_4 = dataframe[date_clmn] <= df_auto_bgt.loc[kk, end_date_clmn]
            dataframe.loc[condition_0 & condition_1 & condition_2 & condition_3 & condition_4,
                          budget_clmn] = df_auto_bgt.loc[kk, auto_bgt_clmn]
        else:
            dataframe.loc[condition_0 & condition_1 & condition_2, budget_clmn] = df_auto_bgt.loc[kk, auto_bgt_clmn]

    # Filter redundant credit card payments and net impacts
    # Filter redundant amazon purchases
    # Re-budget TARGET data
    # Think about what to do with pre-tax deductions
    # Move MN metro to Transportation
    # Included health care to health care
    # Calculate pre-tax benefits
    # Balance from 401k not included on file
    # Implement manual movements between budgets

    # Separate non-assigned items
    df_not_assigned = dataframe[dataframe[budget_clmn].isnull()]
    df_not_assigned.sort_values(by=[description_clmn], inplace=True)

    return [dataframe, df_not_assigned]


def remove_key_duplicates(old_dataframe, key_column):
    new_dataframe = old_dataframe.drop_duplicates(key_column)

    return new_dataframe


def verify_integrity_from_code(code, clmn_list, new_error_list, function_error_str, error_str):
    is_part_of_clmn_list = code in clmn_list
    if not is_part_of_clmn_list:
        new_error_list = new_error_list + function_error_str + variables.setup.column.format(code)
    return new_error_list


def verify_integrity_from_code_lt(clmn_expected_list, clmn_list, error_list, function_error_str, error_str):
    for item in clmn_expected_list:
        error_list = verify_integrity_from_code(item, clmn_list, error_list, function_error_str, error_str)
    return error_list


def gen_delete_list(raw_clmn_list, clmn_expected_list):
    clmns_to_delete = []
    for item in raw_clmn_list:
        is_part_of_expected_list = item in clmn_expected_list
        if not is_part_of_expected_list:
            clmns_to_delete.append(item)
    return clmns_to_delete

def file_clmn_integrity(dataframe, clmn_expected_list, error_str, old_error_list="",
                        function_error_str = "Failed column integrity "):
    raw_clmn_list = dataframe.columns.tolist()
    new_error_list = verify_integrity_from_code_lt(clmn_expected_list, clmn_expected_list, old_error_list,
                                            function_error_str, error_str)
    clmns_to_delete = gen_delete_list(raw_clmn_list, clmn_expected_list)

    return [clmns_to_delete, new_error_list]


def list_differential(old_list, new_list):
    item_diff = []
    are_list_equal = True

    for old_item in old_list:

        if old_item not in new_list:
            item_diff.append(old_item)
            are_list_equal = False

    return [item_diff, are_list_equal]


def generate_wide_clmn_expected_list(old_list, start, end, base_string):
    new_list = old_list
    for ii in range(start, end+1):
        new_list.append(base_string + str(ii))

    return new_list


def generate_wide_clmn_conversion(base_dict, start_month, end_month, base_string):
    final_dict = base_dict
    for jj in range(start_month, end_month+1):
         final_dict[base_string + str(jj)] = str(jj)

    return final_dict


def merge_and_drop(dataframe, df_equalizer, df_merge_clmn, equalizer_merge_clmn, new_clmn_name, code_clmn,
                   error_str, old_error_list="", clm_list="empty"):
    function_error_str = "Failed merge and drop "

    new_error_list = old_error_list
    dataframe = dataframe.merge(df_equalizer, how='left', left_on=df_merge_clmn, right_on=equalizer_merge_clmn)

    if df_merge_clmn in dataframe.columns:
        dataframe = dataframe.drop(df_merge_clmn, axis=1, inplace=False)
    if equalizer_merge_clmn in dataframe.columns:
        dataframe = dataframe.drop(equalizer_merge_clmn, axis=1, inplace=False)
    equalizer_clmn_names = df_equalizer.columns

    dataframe = dataframe.rename(columns={equalizer_clmn_names[1]: new_clmn_name}, inplace=False)

    [dataframe, mid_error_list, are_list_equal] = clean_nan(dataframe, code_clmn, clm_list)

    if not are_list_equal:
        error_str_nan = "PNs with problems: "
        aux = "--"
        new_error_list = new_error_list + aux + error_str + aux + function_error_str + aux + error_str_nan + aux + \
                         str(mid_error_list)

    return [dataframe, new_error_list]


def clean_nan(dataframe, code_clmn, clm_list='empty'):
    if clm_list == 'empty':
        clm_list = dataframe.columns

    old_part_number_bgt_list = dataframe[code_clmn]
    old_part_number_bgt_list = [str(item) for item in old_part_number_bgt_list]

    dataframe = dataframe.dropna(inplace=False, subset=clm_list)

    new_part_number_bgt_list = dataframe[code_clmn]
    new_part_number_bgt_list = [str(item) for item in new_part_number_bgt_list]

    [missing_codes, are_lists_equal] = list_differential(old_part_number_bgt_list, new_part_number_bgt_list)

    return [dataframe, missing_codes, are_lists_equal]


def melt_and_index(wide_dataframe, wide_vars, long_clmn, value_clmn, code_clmn):
    long_dataframe = pd.melt(frame=wide_dataframe, id_vars=wide_vars, value_vars=None, var_name=long_clmn,
                             value_name=value_clmn)
    long_dataframe = long_dataframe.set_index(keys=[code_clmn, long_clmn], drop=True, inplace=False)

    return long_dataframe


def convert_strings_to_nan(item):
    if isinstance(item, str):
        return np.nan
    else:
        return item


def convert_to_numeric(item):
    item = pd.to_numeric(item)

    return item


def convert_to_zero(item):
    if pd.isnull(item):
        item = 0

    return item

def clean_types(dataframe, cleaning_clmns, error_str, old_error_list=""):
    new_dataframe = dataframe
    old_index_list = variables.setup.column.index
    # Use applymap to find str and substitute
    dataframe = dataframe.filter(items=cleaning_clmns, axis=1).applymap(convert_strings_to_nan)
    # delete these rows
    dataframe = dataframe.dropna(axis=0, subset=cleaning_clmns, inplace=False)
    # check which rows where deleted

    new_index_list = variables.setup.column.index
    new_error_list = old_error_list
    [missing_codes, are_lists_equal] = list_differential(old_index_list, new_index_list)

    # save list of deleted of rows into error msg
    if not are_lists_equal:
        error_str_nan = "PNs/month with problems: "
        aux = " -- "
        new_error_list = new_error_list + aux + error_str + aux + error_str_nan + str(missing_codes)

    level_qty = variables.setup.column.index.nlevels

    if level_qty > 1:
        df_missing_codes = pd.DataFrame(missing_codes, columns=['code', 'month'])
        df_missing_codes_list = df_missing_codes['code'].to_list()

        if len(df_missing_codes_list) != 0:
           new_dataframe = new_dataframe.drop(df_missing_codes_list, level=0, axis=0)
    else:
        new_dataframe = new_dataframe.drop(missing_codes, axis=0)

    for item in cleaning_clmns:
        new_dataframe[item] = new_dataframe[item].apply(convert_to_numeric)

    return [new_dataframe, new_error_list]


def convert_nan_to_zero(dataframe, converting_clmns):
    new_dataframe = dataframe

    for item in converting_clmns:
        new_dataframe[item] = new_dataframe[item].apply(convert_to_zero)

    return new_dataframe


def remove_from_list(list, item):
    if item in list:
        list.remove(item)
    return list


def drop_na_crossed(dataframe, ref_clmn, clm_list):
    new_dataframe = dataframe
    dataframe = dataframe.filter(items=clm_list, axis=1)

    old_index_list = dataframe[ref_clmn]

    dataframe = dataframe.dropna(axis=0, inplace=False)

    # is_there_nan = any(pd.isnull(old_index_list))

    old_index_list = dataframe[ref_clmn]
    old_index_list = [str(item) for item in old_index_list]
    old_index_list = remove_from_list(old_index_list, 'nan')

    new_index_list = dataframe[ref_clmn]

    new_index_list = [str(item) for item in new_index_list]

    [missing_codes, are_lists_equal] = list_differential(old_index_list, new_index_list)

    new_dataframe = new_dataframe.drop(missing_codes, axis=0)

    # if is_there_nan:
    #     new_dataframe = new_dataframe.dropna(axis=0, inplace=False)

    return new_dataframe


def clear_extra_rows(dataframe, ref_clmn, error_str, old_error_list="", clm_list='empty'):
    if clm_list == 'empty':
        clm_list = dataframe.columns

    old_index_list = dataframe[ref_clmn]

    dataframe = drop_na_crossed(dataframe, ref_clmn, clm_list)

    new_index_list = dataframe[ref_clmn]

    old_index_list = [str(item) for item in old_index_list]
    new_index_list = [str(item) for item in new_index_list]

    old_index_list = remove_from_list(old_index_list, 'nan')
    [missing_codes, are_lists_equal] = list_differential(old_index_list, new_index_list)

    new_error_list = old_error_list

    if not are_lists_equal:
        error_str_nan = "Rows w/Nan: "
        aux = " -- "
        new_error_list = new_error_list + aux + error_str + aux + error_str_nan + str(missing_codes)

    return [dataframe, new_error_list]


def generate_uom_ref_file(df_base, code_clmn, ref_uom, ref_currency, ref_uom_str, ref_currency_str):
    dataframe = df_base.filter(items=[code_clmn, ref_uom, ref_currency], axis=1)
    dataframe = dataframe.rename(columns={ref_uom: ref_uom_str, ref_currency: ref_currency_str}, inplace=False)

    return dataframe


def prepare_long_uom_ref_file(df_conv_short, code_list, code_clmn, conv_old_uom_clmn, conv_new_uom_clmn,
                                conv_to_all_str):

    query_str1 = code_clmn + '!="' + conv_to_all_str + '"'
    df_conv_long = df_conv_short.query(query_str1, inplace=False)

    query_str2 = code_clmn + '=="' + conv_to_all_str + '"'
    df_conv_short = df_conv_short.query(query_str2, inplace=False)

    for item in code_list:
        df_conv_long = add_all_uom_fx_to_code(df_conv_long, df_conv_short, item, conv_to_all_str)

    df_conv_long = df_conv_long.set_index(keys=[code_clmn, conv_old_uom_clmn, conv_new_uom_clmn], drop=True,
                                          inplace=False)

    return df_conv_long


def add_all_uom_fx_to_code(dataframe, base_dataframe, code, conv_to_all_str):
    base_dataframe = base_dataframe.replace(conv_to_all_str, code, inplace=False)
    dataframe = dataframe.append(base_dataframe)

    return dataframe


def include_predecessors(dataframe, df_pred, pred_code_clmn, extra_clmn_name, code_clmn, month_clmn="empty",
                         bgt_volume_clmn='empty'):
    new_dataframe = dataframe
    new_dataframe[extra_clmn_name] = variables.setup.column.index.get_level_values(code_clmn)

    code_list = variables.setup.column.index

    if month_clmn == "empty":
        for item in code_list:
            new_dataframe = include_single_predecessor_single_index(new_dataframe, df_pred, pred_code_clmn, code_clmn,
                                                                    item)
    else:
        for item in code_list:
            new_dataframe = include_single_predecessor_double_index(new_dataframe, df_pred, pred_code_clmn, code_clmn,
                                                                    month_clmn, item, bgt_volume_clmn)

    return new_dataframe


def include_single_predecessor_single_index(dataframe, df_pred, pred_code_clmn, code_clmn, new_code):

    bgt_pred = df_pred.loc[new_code, pred_code_clmn]

    add_row = dataframe.reset_index(inplace=False)
    add_row = add_row[add_row.apply(lambda x: x[code_clmn] == bgt_pred, axis=1)]
    add_row[code_clmn] = new_code
    add_row = add_row.set_index(keys=[code_clmn], drop=True, inplace=False)

    new_dataframe = dataframe.append(add_row)

    return new_dataframe


def include_single_predecessor_double_index(dataframe, df_pred, pred_code_clmn, code_clmn, month_clmn, new_code,
                                            bgt_volume_clmn):
    new_dataframe = dataframe

    bgt_pred = df_pred.loc[new_code, pred_code_clmn]

    bgt_pred_array = [12 * [bgt_pred],
                      ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12']]

    bgt_pred_tuple = list(zip(*bgt_pred_array))

    add_row = new_dataframe.filter(bgt_pred_tuple, axis=0)
    add_row = add_row.reset_index(inplace=False)
    add_row[code_clmn] = new_code
    if bgt_volume_clmn != 'empty':
        add_row[bgt_volume_clmn] = 0
    add_row = add_row.set_index(keys=[code_clmn, month_clmn], drop=True, inplace=False)

    new_dataframe = new_dataframe.append(add_row)

    return new_dataframe


def generate_price_curve_based_on_constant(old_dataframe, value_list, start_month, end_month, df_id_vars, month_clmn,
                                           price_clmn, code_clmn, strategy_clmn, strategy_str, desired_clmn_list):
    month_list = range(start_month, end_month+1)
    new_dataframe = old_dataframe
    new_dataframe = new_dataframe.rename(columns={price_clmn: str(month_list[0])}, inplace=False)

    for item in month_list:
        new_dataframe[str(item)] = value_list

    new_dataframe = generate_price_curve_based_on_curve(new_dataframe, df_id_vars, month_clmn, price_clmn, code_clmn,
                                                        strategy_clmn, strategy_str, desired_clmn_list)

    return new_dataframe


def generate_price_curve_based_on_curve(old_dataframe, df_id_vars, month_clmn, price_clmn, code_clmn, strategy_clmn,
                                        strategy_str, desired_clmn_list):
    # wide to long
    new_dataframe = melt_and_index(old_dataframe, df_id_vars, month_clmn, price_clmn, code_clmn)
    # add column with forecast strategy
    new_dataframe[strategy_clmn] = strategy_str
    new_dataframe = new_dataframe.filter(items=desired_clmn_list, axis=1)

    return new_dataframe


def generate_price_curve_based_on_budget(old_dataframe, start_month, end_month, month_clmn, price_clmn, code_clmn,
                                         strategy_clmn, strategy_str, ref_file, ref_clmn_lt, matching_tuple,
                                         desired_clmn_list):
    # forecast as a budget file
    old_df_clmn_lt = old_dataframe.columns
    month_list = range(start_month, end_month + 1)
    wide_dataframe = old_dataframe
    value = 0

    for item in month_list:
        wide_dataframe[str(item)] = value

    long_dataframe = melt_and_index(wide_dataframe, old_df_clmn_lt, month_clmn, price_clmn, code_clmn)
    long_dataframe = long_dataframe.drop(columns=price_clmn, inplace=False)

    # budget file
    ref_data = ref_file.filter(items=ref_clmn_lt, axis=1)
    ref_data = ref_data.rename(columns=matching_tuple, inplace=False)

    new_dataframe = long_dataframe.merge(ref_data, how='left', left_index=True, right_index=True)
    new_dataframe[strategy_clmn] = strategy_str
    new_dataframe = new_dataframe.filter(items=desired_clmn_list, axis=1)

    return new_dataframe


def generate_price_curve_based_on_actuals(old_dataframe, start_month, end_month, month_clmn, price_clmn, code_clmn,
                                          strategy_clmn, strategy_str, ref_file, ref_clmn_lt, matching_tuple,
                                          act_price_clmn, act_volume_clmn, desired_clmn_list):

    old_df_clmn_lt = old_dataframe.columns
    month_list = range(start_month, end_month + 1)
    wide_dataframe = old_dataframe
    value = 0

    for item in month_list:
        wide_dataframe[str(item)] = value

    long_dataframe = melt_and_index(wide_dataframe, old_df_clmn_lt, month_clmn, price_clmn, code_clmn)
    long_dataframe = long_dataframe.drop(columns=price_clmn, inplace=False)

    # budget file
    ref_data = ref_file.filter(items=ref_clmn_lt, axis=1)

    df_avg = ref_data

    ref_data = ref_data.drop(columns=act_price_clmn)
    spend_clmn = 'act_spend'
    df_avg[spend_clmn] = df_avg[act_volume_clmn] * df_avg[act_price_clmn]
    # filter data (only code and PN)

    df_avg = df_avg.groupby(level=code_clmn).sum()
    df_avg[act_price_clmn] = df_avg[spend_clmn] / df_avg[act_volume_clmn]
    df_avg = df_avg.filter(items=[act_price_clmn], axis=1)

    df_avg = df_avg.reset_index(inplace=False)
    ref_data = ref_data.reset_index(inplace=False)
    ref_data = ref_data.drop(columns=month_clmn)

    ref_data = ref_data.merge(df_avg, how='left', on=code_clmn)
    ref_data = remove_key_duplicates(ref_data, code_clmn)

    long_dataframe = long_dataframe.reset_index(inplace=False)

    new_dataframe = long_dataframe.merge(ref_data, how='left', on=code_clmn)

    new_dataframe = new_dataframe.rename(columns=matching_tuple, inplace=False)
    new_dataframe = new_dataframe.set_index(keys=[code_clmn, month_clmn], drop=True, inplace=False)

    new_dataframe[strategy_clmn] = strategy_str
    new_dataframe = new_dataframe.filter(items=desired_clmn_list, axis=1)


    return new_dataframe


def generate_price_curve_based_on_inflation(old_dataframe, start_month, end_month, month_clmn, price_clmn, code_clmn,
                                            base_price_clmn, inflation_clmn, inf_month_clmn, strategy_clmn,
                                            strategy_str, desired_clmn_list):

    # add column with inflated price
    wide_dataframe = old_dataframe
    inf_price_clmn = 'inf_price'
    wide_dataframe[inf_price_clmn] = wide_dataframe[base_price_clmn] * (1 + wide_dataframe[inflation_clmn])

    # create month columns from report month+1 until 12
    old_df_clmn_lt = old_dataframe.columns
    month_list = range(start_month, end_month + 1)
    wide_dataframe = old_dataframe
    value = 0

    for item in month_list:
        wide_dataframe[str(item)] = value

    # wide to long
    new_dataframe = melt_and_index(wide_dataframe, old_df_clmn_lt, month_clmn, price_clmn, code_clmn)
    new_dataframe = new_dataframe.drop(columns=price_clmn, inplace=False)

    new_dataframe = new_dataframe.reset_index(inplace=False)
    new_dataframe[month_clmn] = new_dataframe[month_clmn].astype(int)

    new_dataframe.loc[new_dataframe[month_clmn] < new_dataframe[inf_month_clmn], price_clmn] = new_dataframe[base_price_clmn]
    new_dataframe.loc[new_dataframe[month_clmn] >= new_dataframe[inf_month_clmn], price_clmn] = new_dataframe[inf_price_clmn]

    new_dataframe[strategy_clmn] = strategy_str
    new_dataframe[month_clmn] = new_dataframe[month_clmn].astype(str)
    new_dataframe = new_dataframe.set_index(keys=[code_clmn, month_clmn], drop=True, inplace=False)
    new_dataframe = new_dataframe.filter(items=desired_clmn_list, axis=1)
    # drop base price and inflation column

    return new_dataframe


def add_category_to_frc(old_dataframe, category_dataframe, code_clmn, month_clmn, category_clmn):

    category_dataframe = category_dataframe.reset_index(inplace=False)
    category_dataframe = category_dataframe.filter(items=[code_clmn, month_clmn, category_clmn], axis=1)
    category_dataframe = category_dataframe.set_index(keys=[code_clmn, month_clmn], drop=True, inplace=False)
    new_dataframe = old_dataframe.merge(category_dataframe, how='inner', left_index=True, right_index=True)

    return new_dataframe


def fix_index(old_dataframe, code_clmn, month_clmn):
    new_dataframe = old_dataframe

    new_dataframe = new_dataframe.reset_index(inplace=False)
    new_dataframe[month_clmn] = new_dataframe[month_clmn].apply(convert_to_numeric)

    new_dataframe = new_dataframe.set_index(keys=[code_clmn, month_clmn], drop=True, inplace=False)

    return new_dataframe


#############################              2) CALCULATION ENGINE                  ######################################

def convert_uom(dataframe, df_conv, old_uom_list, new_uom_list, mult_list, conv_multiplier_clmn, code_clmn,
                month_clmn='empty'):

    old_price_value_clmn = old_uom_list[0]
    old_price_uom_clmn = old_uom_list[1]
    old_price_per_clmn = old_uom_list[2]
    old_price_currency_clmn = old_uom_list[3]
    old_volume_value_clmn = old_uom_list[4]
    old_volume_uom_clmn = old_uom_list[5]
    old_volume_per_clmn = old_uom_list[6]

    new_price_value_clmn = new_uom_list[0]
    new_price_uom_clmn = new_uom_list[1]
    new_price_per_clmn = new_uom_list[2]
    new_price_currency_clmn = new_uom_list[3]
    new_volume_value_clmn = new_uom_list[4]
    new_volume_uom_clmn = new_uom_list[5]
    new_volume_per_clmn = new_uom_list[6]

    mult_price_uom_clmn = mult_list[0]
    mult_price_per_clmn = mult_list[1]
    mult_price_currency_clmn = mult_list[2]
    mult_volume_uom_clmn = mult_list[3]
    mult_volume_per_clmn = mult_list[4]

    dataframe.reset_index(inplace=True)

    dataframe = find_multiplier(dataframe, df_conv, code_clmn, old_price_uom_clmn, new_price_uom_clmn,
                                conv_multiplier_clmn, mult_price_uom_clmn)
    dataframe[mult_price_uom_clmn] = 1/dataframe[mult_price_uom_clmn]

    dataframe[mult_price_per_clmn] = dataframe[new_price_per_clmn]/dataframe[old_price_per_clmn]

    dataframe = find_multiplier(dataframe, df_conv, code_clmn, old_price_currency_clmn, new_price_currency_clmn,
                                conv_multiplier_clmn, mult_price_currency_clmn)

    dataframe[new_price_value_clmn] = dataframe[old_price_value_clmn] * dataframe[mult_price_uom_clmn] * \
                                      dataframe[mult_price_per_clmn] * dataframe[mult_price_currency_clmn]

    if mult_volume_per_clmn != 'empty':
        dataframe = find_multiplier(dataframe, df_conv, code_clmn, old_volume_uom_clmn, new_volume_uom_clmn,
                                    conv_multiplier_clmn, mult_volume_uom_clmn)
        dataframe[mult_volume_per_clmn] = dataframe[old_volume_per_clmn]/dataframe[new_volume_per_clmn]
        dataframe[new_volume_value_clmn] = dataframe[old_volume_value_clmn] * dataframe[mult_volume_uom_clmn] * \
                                           dataframe[mult_volume_per_clmn]

    if month_clmn != 'empty':
        dataframe.set_index(keys=[code_clmn, month_clmn], drop=True, inplace=True)
    else:
        dataframe.set_index(keys=code_clmn, drop=True, inplace=True)

    return dataframe


def find_multiplier(dataframe, df_conv, code_clmn, old_clmn, new_clmn, old_name_clmn, new_name_clmn):

    dataframe = dataframe.merge(df_conv, how='left', left_on=[code_clmn, old_clmn, new_clmn], right_index=True)
    dataframe.rename(columns={old_name_clmn: new_name_clmn}, inplace=True)

    return dataframe


def clean_nan_engine(dataframe, code_clmn, month_clmn, error_str, old_error_list='empty', clm_list='empty'):
    if clm_list == 'empty':
        clm_list = dataframe.columns

    dataframe.reset_index(inplace=True)

    old_part_number_bgt_list = dataframe[code_clmn]
    old_part_number_bgt_list = set(old_part_number_bgt_list)

    old_part_number_bgt_list = [str(item) for item in old_part_number_bgt_list]

    dataframe = dataframe.dropna(inplace=False, subset=clm_list)

    new_part_number_bgt_list = dataframe[code_clmn]
    new_part_number_bgt_list = set(new_part_number_bgt_list)

    new_part_number_bgt_list = [str(item) for item in new_part_number_bgt_list]

    [missing_codes, are_lists_equal] = list_differential(old_part_number_bgt_list, new_part_number_bgt_list)

    new_error_list = old_error_list

    if not are_lists_equal:
        error_str_nan = "Rows w/Nan: "
        aux = " -- "
        new_error_list = new_error_list + aux + error_str + aux + error_str_nan + str(missing_codes)

    dataframe.set_index(keys=[code_clmn, month_clmn], drop=True, inplace=True)

    return [dataframe, new_error_list]


def get_max_amount_of_code_repetitions(dataframe):
    code_list = variables.setup.column.index.tolist()

    code_qty = []
    for item in code_list:
        code_qty.append(code_list.count(item))

    max_qty = max(code_qty)

    return max_qty


def generate_prj_clm_list(proj_str, layer_qty):
    proj_str_lt = []

    for i in range(1, layer_qty+1):
        proj_str_lt.append(proj_str+str(i))

    return proj_str_lt


def generate_generic_project_info(dataframe, category_clmn, pj_code_clmn, pj_name_clmn, pj_value_clmn, gen_code_str,
                                  gen_name_str, code_clmn, month_clmn):

    category_list = set(dataframe[category_clmn])

    pj_code_list = generate_code_list(len(category_list), gen_code_str)
    pj_name_list = generate_name_list(category_list, gen_name_str)

    df_gen = pd.DataFrame(category_list, columns=[category_clmn])
    df_gen[pj_code_clmn] = pj_code_list
    df_gen[pj_name_clmn] = pj_name_list
    df_gen[pj_value_clmn] = 0

    dataframe.reset_index(inplace=True)
    dataframe = dataframe.merge(df_gen, how='left', left_on=category_clmn, right_on=category_clmn)
    dataframe.set_index(keys=[code_clmn, month_clmn], drop=True, inplace=True)

    return dataframe


def generate_name_list(category_list, gen_name_str):
    name_list = []

    for item in category_list:
        name_list.append(item+gen_name_str)

    return name_list


def generate_code_list(category_list_size, gen_code_str):
    code_list = []

    for ii in range(1, category_list_size+1):
        if ii < 10:
            code_list.append(gen_code_str + '00' + str(ii))
        elif ii < 100:
            code_list.append(gen_code_str + '0' + str(ii))
        else:
            code_list.append(gen_code_str + str(ii))

    return code_list


def add_projects(dataframe, df_project,  pj_description_list, pj_value_list, pj_code_list, code_clmn, month_clmn,
                 pjmp_code_clmn, pjmp_description_clmn, pcent_clmn, cnst_clmn_at_ref, start_mth_clmn, bsl_sav_clmn,
                 volume_at_ref_clmn, cnst_per_clmn, volume_per_clmn_at_ref, pj_gen_value_clmn, layers, delete_list):

    df_project.reset_index(inplace=True)
    df_remainder_pj = df_project
    dataframe[pj_gen_value_clmn] = dataframe[bsl_sav_clmn]

    for kk in range(0, layers):

        [df_clean_pj, df_remainder_pj] = remove_project_duplicates(df_remainder_pj, code_clmn)

        dataframe.reset_index(inplace=True)
        dataframe = dataframe.merge(df_clean_pj, how='left', left_on=code_clmn, right_on=code_clmn)
        dataframe[pj_value_list[kk]] = dataframe.apply(lambda x: calculate_project(x[bsl_sav_clmn],
                                                                                   x[volume_at_ref_clmn],
                                                                                   x[pcent_clmn], x[cnst_clmn_at_ref],
                                                                                   x[start_mth_clmn], x[month_clmn],
                                                                                   x[cnst_per_clmn],
                                                                                   x[volume_per_clmn_at_ref]),
                                                                                  axis=1)
        dataframe.set_index(keys=[code_clmn, month_clmn], drop=True, inplace=True)
        rename_base = {pjmp_code_clmn: pj_code_list[kk],
                       pjmp_description_clmn: pj_description_list[kk]}
        dataframe = dataframe.rename(columns=rename_base, inplace=False)
        dataframe.drop(delete_list, axis=1, inplace=True)
        dataframe[pj_gen_value_clmn] = dataframe[pj_gen_value_clmn] - dataframe[pj_value_list[kk]]

    return dataframe


def calculate_project(savings, volume, pcent, cnst, start_mth, month, cnst_per, vol_per):

    if convert_to_numeric(start_mth) > convert_to_numeric(month):
        value = 0
    else:
        value_cnst = volume/vol_per * cnst/cnst_per
        value_pcent = pcent * savings

        value_cnst = 0 if np.isnan(value_cnst) else value_cnst
        value_pcent = 0 if np.isnan(value_pcent) else value_pcent

        value = value_cnst + value_pcent

    return value


def remove_project_duplicates(df_pj, code_column):
    df_clean_pj = remove_key_duplicates(df_pj, code_column)

    [missing_codes, are_lists_equal] = list_differential(variables.setup.column.index, variables.setup.column.index)
    df_remainder_pj =df_pj.filter(items=missing_codes, axis=0)

    return [df_clean_pj, df_remainder_pj]


def create_project_dataframe(df_pj, df_cy, pj_desired_clmn_list, proj_str_description_lt, proj_str_value_lt,
                             proj_str_code_lt, pj_code_clmn, pj_value_clmn, new_str_clmn_list, uom_ref_currency_clmn):
    pj_code = new_str_clmn_list[0]
    pj_description = new_str_clmn_list[1]
    pj_value = new_str_clmn_list[2]

    df_pj.reset_index(inplace=True)
    df_pj = df_pj[pj_desired_clmn_list]

    df_cy = df_cy.reset_index(inplace=False)
    df_cy = df_cy[proj_str_description_lt + proj_str_value_lt + proj_str_code_lt + [uom_ref_currency_clmn]]

    cy_len = int(df_cy.shape[1]/3)
    short_clmn_list = [proj_str_code_lt[0], proj_str_description_lt[0], proj_str_value_lt[0], uom_ref_currency_clmn]
    new_cy = df_cy[short_clmn_list]
    new_cy = new_cy.rename(columns={proj_str_code_lt[0]: pj_code,
                                    proj_str_description_lt[0]: pj_description,
                                    proj_str_value_lt[0]:  pj_value}, inplace=False)

    for kk in range(1, cy_len):
        short_clmn_list = [proj_str_code_lt[kk], proj_str_description_lt[kk], proj_str_value_lt[kk],
                           uom_ref_currency_clmn]
        add_cy = df_cy[short_clmn_list]
        add_cy = add_cy.rename(columns={proj_str_code_lt[kk]: pj_code,
                                        proj_str_description_lt[kk]: pj_description,
                                        proj_str_value_lt[kk]:  pj_value}, inplace=False)
        new_cy = new_cy.append(add_cy)

    new_cy = pd.pivot_table(new_cy, values=pj_value, index=[pj_code, pj_description], aggfunc=np.sum)
    new_cy.reset_index(inplace=True)

    df_pj = df_pj.drop_duplicates(pj_code_clmn)

    df_pj = new_cy.merge(df_pj, how='left', left_on=pj_code, right_on=pj_code_clmn)
    df_pj.set_index(keys=pj_code, drop=True, inplace=True)

    return df_pj


def add_project_to_clmn_list(clmn_list, pj_str_code_lt, pj_str_description_lt, pj_str_value_lt, pj_layer):

    for kk in range(0, pj_layer+1):
        clmn_list.append(pj_str_code_lt[kk])
        clmn_list.append(pj_str_description_lt[kk])
        clmn_list.append(pj_str_value_lt[kk])

    return clmn_list


def save_excel(filename, df_cy, df_pj, sheetname_cy, sheetname_pj, color_list, cy_color_index_for_clmns, cy_color_order,
               pj_color_index_for_clmns, pj_color_order):

    # Create a Pandas Excel writer using XlsxWriter as the engine.
    index_qty_cy = len(variables.setup.column.index.standard_clmn_list)
    index_qty_pj = len(variables.setup.column.index.standard_clmn_list)

    cy_color_index_for_clmns.append(index_qty_cy)
    cy_color_index_for_clmns.sort()

    pj_color_index_for_clmns.append(index_qty_pj)
    pj_color_index_for_clmns.sort()

    writer = pd.ExcelWriter(filename, engine='xlsxwriter')

    # Convert the dataframe to an XlsxWriter Excel object.
    df_cy.to_excel(writer, sheet_name=sheetname_cy, float_format="%.4f", startrow=1, startcol=0, merge_cells=False,
                   freeze_panes={1, 2}, header=False)
    df_pj.to_excel(writer, sheet_name=sheetname_pj, float_format="%.4f", startrow=1, startcol=0, merge_cells=False,
                   header=False)

    # Get the xlsxwriter workbook and worksheet objects.
    workbook = writer.book
    worksheet_cy = writer.sheets[sheetname_cy]
    worksheet_pj = writer.sheets[sheetname_pj]

    # first line in bold
    header_format = workbook.add_format({'align': 'center',
                                         'border': 1,
                                         'valign': 'vcenter',
                                         'bold': True,
                                         'text_wrap': True})

    # Write the column headers with the defined format.

    for col_num, value in enumerate(df_cy.columns.values):
        worksheet_cy.write(0, col_num + index_qty_cy, value, header_format)

    for col_num, value in enumerate(df_pj.columns.values):
        worksheet_pj.write(0, col_num + index_qty_pj, value, header_format)


    index_format = workbook.add_format({'align': 'center',
                                        'text_wrap': True,
                                        'border': 1,
                                        'valign': 'middle',
                                        'num_format': '#,##0.00'})
    worksheet_cy.set_column(0, index_qty_cy-1, 10, index_format)
    worksheet_pj.set_column(0, index_qty_pj - 1, 10, index_format)

    color0_format = workbook.add_format({'align': 'center',
                                         'text_wrap': True,
                                         'border': 1,
                                         'valign': 'middle',
                                         'bg_color': color_list[0],
                                         'num_format': '#,##0.00000'})

    color1_format = workbook.add_format({'align': 'center',
                                         'text_wrap': True,
                                         'border': 1,
                                         'valign': 'middle',
                                         'bg_color': color_list[1],
                                         'num_format': '#,##0.00000'})

    color2_format = workbook.add_format({'align': 'center',
                                         'text_wrap': True,
                                         'border': 1,
                                         'valign': 'middle',
                                         'bg_color': color_list[2],
                                         'num_format': '#,##0.00000'})

    color3_format = workbook.add_format({'align': 'center',
                                         'text_wrap': True,
                                         'border': 1,
                                         'valign': 'middle',
                                         'bg_color': color_list[3],
                                         'num_format': '#,##0.00000'})

    color4_format = workbook.add_format({'align': 'center',
                                         'text_wrap': True,
                                         'border': 1,
                                         'valign': 'middle',
                                         'bg_color': color_list[4],
                                         'num_format': '#,##0.00000'})

    format_list = [color0_format, color1_format, color2_format, color3_format, color4_format]

    cy_index_clmn = len(cy_color_index_for_clmns)
    for kk in range(1, cy_index_clmn):
        worksheet_cy.set_column(cy_color_index_for_clmns[kk-1], cy_color_index_for_clmns[kk]-1, 25,
                                format_list[cy_color_order[kk-1]])

    pj_index_clmn = len(pj_color_index_for_clmns)
    for kk in range(1, pj_index_clmn):
        worksheet_pj.set_column(pj_color_index_for_clmns[kk - 1], pj_color_index_for_clmns[kk] - 1, 25,
                                format_list[pj_color_order[kk - 1]])

    writer.save()

    return
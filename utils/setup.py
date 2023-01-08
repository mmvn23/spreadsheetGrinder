import pandas as pd
import variables.setup.file as stp

import variables.setup.column
import variables.var_column as clmn
import utils.general as ut


def load_xlsx(filepath, sheet_name, header, new_column_list, key_clmn_list, index_col, usecols, skiprows):
    any_dataframe = pd.read_excel(filepath, sheet_name=sheet_name, header=header, index_col=index_col,
                                  usecols=usecols, skiprows=skiprows, engine='openpyxl')
    any_dataframe.rename(columns=dict(zip(any_dataframe.columns, new_column_list)), inplace=True)

    if len(key_clmn_list) > 0:
        any_dataframe.set_index(key_clmn_list, inplace=True)

    return any_dataframe


def load_csv(filepath):
    any_dataframe = pd.read_csv(filepath, header=0, encoding=stp.encoding_to_save)
    return any_dataframe


def prepare_df_column_setup(dataset_name, any_df_setup, setup_clmn_list_for_clmn_info, any_df_column):
    df_column_setup = ut.get_value_from_dataframe(any_df_setup, target_column_list=setup_clmn_list_for_clmn_info,
                                                  column_list_to_filter=[variables.setup.column.dataset_name],
                                                  value_list_to_filter=[dataset_name],
                                                  return_dataframe=True)
    df_column_setup.set_index(variables.setup.column.row, inplace=True)
    df_column_setup.drop(columns=[variables.setup.column.dataset_name], inplace=True, axis=1)
    df_column_setup = df_column_setup.transpose()
    df_column_setup = df_column_setup.dropna(how='all', axis=0)
    df_column_setup.reset_index(inplace=True)
    df_column_setup.drop(columns=['index'], inplace=True, axis=1)
    df_column_setup.rename({variables.setup.column.main: variables.setup.column.clmn_var_name}, inplace=True, axis=1)

    if variables.setup.column.multiplier in df_column_setup.columns:
        df_column_setup[variables.setup.column.multiplier].fillna(value=1, axis=0, inplace=True)

    df_column_setup.set_index(variables.setup.column.clmn_var_name, inplace=True)
    df_column_setup = df_column_setup.merge(any_df_column, how='left', left_index=True, right_index=True)

    return df_column_setup



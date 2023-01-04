import utils.setup
import variables.setup.column
import variables.setup.file as stp
import variables.var_column as clmn
import pandas as pd
from Dataset.Raw import RawDataset
from Dataset.Matrix import DataMatrix

pd.options.display.float_format = '{:.2f}'.format
pd.options.display.max_rows = 20000
pd.set_option('display.max_columns', 100)


def pre_load_objects_raw(df_column):
    df_setup = utils.setup.load_xlsx(filepath=stp.filepath,
                                     sheet_name=stp.setup_raw_sheet_name,
                                     header=stp.header,
                                     new_column_list=[variables.setup.column.row, variables.setup.column.dataset_name,
                                                      variables.setup.column.source_name,
                                                      variables.setup.column.source_last_update,
                                                      variables.setup.column.clmn1, variables.setup.column.clmn2,
                                                      variables.setup.column.clmn3, variables.setup.column.clmn4,
                                                      variables.setup.column.clmn5, variables.setup.column.clmn6,
                                                      variables.setup.column.clmn7, variables.setup.column.clmn8,
                                                      variables.setup.column.clmn9, variables.setup.column.clmn10,
                                                      variables.setup.column.clmn11, variables.setup.column.clmn12,
                                                      variables.setup.column.clmn13, variables.setup.column.clmn14,
                                                      variables.setup.column.clmn15,
                                                      variables.setup.column.clmn16, variables.setup.column.clmn17,
                                                      variables.setup.column.clmn18, variables.setup.column.clmn19,
                                                      variables.setup.column.clmn20,
                                                      variables.setup.column.to_transpose, variables.setup.column.root,
                                                      variables.setup.column.folder, variables.setup.column.file,
                                                      variables.setup.column.sheet, variables.setup.column.format,
                                                      variables.setup.column.usecols,
                                                      variables.setup.column.nrows, variables.setup.column.date_parser,
                                                      variables.setup.column.skiprow_bf_header,
                                                      variables.setup.column.skiprow_af_header,
                                                      variables.setup.column.encoding, variables.setup.column.decimal],
                                     key_clmn_list=[variables.setup.column.dataset_name, variables.setup.column.row],
                                     index_col=stp.index_col,
                                     usecols=stp.usecols,
                                     skiprows=stp.skiprows)
    # df_setup.reset_index(inplace=True)
    # df_setup.rename(columns={variables.setup.column.source_name: clmn.source_name,
    #                           variables.setup.column.source_last_update: clmn.source_last_update},
    #                  inplace=True)
    # print('PRE_LOADING line 48')
    # print(df_setup)
    # df_setup.set_index([variables.setup.column.row, variables.setup.column.dataset_name], inplace=True)
    any_raw_dataset_list = RawDataset.load_list_of_datasets(df_setup, df_column)

    for any_raw_dataset in any_raw_dataset_list:
        any_raw_dataset.write()

    return


def pre_load_mtx_objects(df_column):
    df_setup = utils.setup.load_xlsx(filepath=stp.filepath,
                                     sheet_name=stp.setup_mtx_sheet_name,
                                     header=stp.header,
                                     new_column_list=[variables.setup.column.row, variables.setup.column.dataset_name,
                                                      variables.setup.column.clmn1, variables.setup.column.clmn2,
                                                      variables.setup.column.clmn3, variables.setup.column.clmn4,
                                                      variables.setup.column.clmn5,
                                                      variables.setup.column.clmn6, variables.setup.column.clmn7,
                                                      variables.setup.column.clmn8,
                                                      variables.setup.column.clmn9, variables.setup.column.clmn10,
                                                      variables.setup.column.clmn11, variables.setup.column.clmn12,
                                                      variables.setup.column.clmn13,
                                                      variables.setup.column.clmn14, variables.setup.column.clmn15,
                                                      variables.setup.column.clmn16, variables.setup.column.clmn17,
                                                      variables.setup.column.clmn18, variables.setup.column.clmn19,
                                                      variables.setup.column.clmn20,
                                                      variables.setup.column.root, variables.setup.column.folder],
                                     key_clmn_list=[variables.setup.column.dataset_name, variables.setup.column.row],
                                     index_col=stp.index_col,
                                     usecols=stp.usecols,
                                     skiprows=stp.skiprows)
    # print('PRE_LOADING line 48')
    # print(df_setup)
    # df_setup.set_index([variables.setup.column.row, variables.setup.column.dataset_name], inplace=True)
    any_datamatrix_list = DataMatrix.load_list_of_datasets(df_setup, df_column)

    for any_datamatrix in any_datamatrix_list:
        any_datamatrix.write()

    return


def pre_load_objects():
    df_column = utils.setup.load_xlsx(filepath=stp.filepath,
                                      sheet_name=stp.column_sheet_name,
                                      header=stp.header,
                                      new_column_list=[variables.setup.column.clmn_var_name,
                                                       variables.setup.column.clmn_rep_name,
                                                       variables.setup.column.type,
                                                       variables.setup.column.uom_conversion_to_be_applied,
                                                       variables.setup.column.nomenclature_to_be_applied,
                                                       variables.setup.column.string_cleaning_to_be_applied],
                                      key_clmn_list=[variables.setup.column.clmn_var_name],
                                      index_col=stp.index_col,
                                      usecols=stp.usecols,
                                      skiprows=stp.skiprows)
    # df_column.reset_index(inplace=True)
    # df_column.rename(columns={variables.setup.column.clmn_var_name: clmn.clmn_var_name,
    #                   variables.setup.column.clmn_rep_name: clmn.clmn_rep_name,
    #                   variables.setup.column.type: clmn.my_type,
    #                   variables.setup.column.uom_conversion_to_be_applied: clmn.uom_conversion_to_be_applied,
    #                   variables.setup.column.nomenclature_to_be_applied: clmn.nomenclature_to_be_applied,
    #                   variables.setup.column.string_cleaning_to_be_applied: clmn.string_cleaning_to_be_applied},
    #                  inplace=True)
    # df_column.set_index(clmn.clmn_var_name, inplace=True)

    pre_load_objects_raw(df_column)
    pre_load_mtx_objects(df_column)
    return


pre_load_objects()



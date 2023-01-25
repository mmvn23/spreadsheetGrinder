import utils.setup
import variables.setup_column as stp_clmn
import variables.PERSONAL_setup.file as ps_stp
import variables.DIAGEO_setup.file as dg_stp
import variables.var_column as clmn
import pandas as pd
from Dataset.Raw import RawDataset
from Dataset.Matrix import DataMatrix

pd.options.display.float_format = '{:.2f}'.format
pd.options.display.max_rows = 20000
pd.set_option('display.max_columns', 100)


def pre_load_objects_raw(df_column, root_json, folder_json):
    df_setup = utils.setup.load_xlsx(filepath=ps_stp.filepath,
                                     sheet_name=ps_stp.setup_raw_sheet_name,
                                     header=ps_stp.header,
                                     new_column_list=[stp_clmn.row, stp_clmn.dataset_name,
                                                      stp_clmn.source_name,
                                                      stp_clmn.source_last_update,
                                                      stp_clmn.clmn1, stp_clmn.clmn2,
                                                      stp_clmn.clmn3, stp_clmn.clmn4,
                                                      stp_clmn.clmn5, stp_clmn.clmn6,
                                                      stp_clmn.clmn7, stp_clmn.clmn8,
                                                      stp_clmn.clmn9, stp_clmn.clmn10,
                                                      stp_clmn.clmn11, stp_clmn.clmn12,
                                                      stp_clmn.clmn13, stp_clmn.clmn14,
                                                      stp_clmn.clmn15,
                                                      stp_clmn.clmn16, stp_clmn.clmn17,
                                                      stp_clmn.clmn18, stp_clmn.clmn19,
                                                      stp_clmn.clmn20,
                                                      stp_clmn.to_transpose, stp_clmn.root,
                                                      stp_clmn.folder, stp_clmn.file,
                                                      stp_clmn.sheet, stp_clmn.format,
                                                      stp_clmn.usecols,
                                                      stp_clmn.nrows, stp_clmn.date_parser,
                                                      stp_clmn.skiprow_bf_header,
                                                      stp_clmn.skiprow_af_header,
                                                      stp_clmn.encoding, stp_clmn.decimal],
                                     key_clmn_list=[stp_clmn.dataset_name, stp_clmn.row],
                                     index_col=ps_stp.index_col,
                                     usecols=ps_stp.usecols,
                                     skiprows=ps_stp.skiprows)
    # df_setup.reset_index(inplace=True)
    # df_setup.rename(columns={stp_clmn.source_name: clmn.source_name,
    #                           stp_clmn.source_last_update: clmn.source_last_update},
    #                  inplace=True)
    # print('PRE_LOADING line 48')
    # print(df_setup)
    # df_setup.set_index([stp_clmn.row, stp_clmn.dataset_name], inplace=True)
    any_raw_dataset_list = RawDataset.load_list_of_datasets(df_setup, df_column)

    for any_raw_dataset in any_raw_dataset_list:
        any_raw_dataset.write(root=root_json, folder=folder_json)

    return


def pre_load_mtx_objects(df_column, root_json, folder_json):
    df_setup = utils.setup.load_xlsx(filepath=ps_stp.filepath,
                                     sheet_name=ps_stp.setup_mtx_sheet_name,
                                     header=ps_stp.header,
                                     new_column_list=[stp_clmn.row, stp_clmn.dataset_name,
                                                      stp_clmn.clmn1, stp_clmn.clmn2,
                                                      stp_clmn.clmn3, stp_clmn.clmn4,
                                                      stp_clmn.clmn5,
                                                      stp_clmn.clmn6, stp_clmn.clmn7,
                                                      stp_clmn.clmn8,
                                                      stp_clmn.clmn9, stp_clmn.clmn10,
                                                      stp_clmn.clmn11, stp_clmn.clmn12,
                                                      stp_clmn.clmn13,
                                                      stp_clmn.clmn14, stp_clmn.clmn15,
                                                      stp_clmn.clmn16, stp_clmn.clmn17,
                                                      stp_clmn.clmn18, stp_clmn.clmn19,
                                                      stp_clmn.clmn20,
                                                      stp_clmn.root, stp_clmn.folder],
                                     key_clmn_list=[stp_clmn.dataset_name, stp_clmn.row],
                                     index_col=ps_stp.index_col,
                                     usecols=ps_stp.usecols,
                                     skiprows=ps_stp.skiprows)
    # print('PRE_LOADING line 48')
    # print(df_setup)
    # df_setup.set_index([stp_clmn.row, stp_clmn.dataset_name], inplace=True)
    any_datamatrix_list = DataMatrix.load_list_of_datasets(df_setup, df_column)

    for any_datamatrix in any_datamatrix_list:
        any_datamatrix.write(root=root_json, folder=folder_json)

    return


def pre_load_objects():
    df_column = utils.setup.load_xlsx(filepath=ps_stp.filepath,
                                      sheet_name=ps_stp.column_sheet_name,
                                      header=ps_stp.header,
                                      new_column_list=[stp_clmn.clmn_var_name,
                                                       stp_clmn.clmn_rep_name,
                                                       stp_clmn.type,
                                                       stp_clmn.uom_conversion_to_be_applied,
                                                       stp_clmn.nomenclature_to_be_applied,
                                                       stp_clmn.string_cleaning_to_be_applied],
                                      key_clmn_list=[stp_clmn.clmn_var_name],
                                      index_col=ps_stp.index_col,
                                      usecols=ps_stp.usecols,
                                      skiprows=ps_stp.skiprows)
    # df_column.reset_index(inplace=True)
    # df_column.rename(columns={stp_clmn.clmn_var_name: clmn.clmn_var_name,
    #                   stp_clmn.clmn_rep_name: clmn.clmn_rep_name,
    #                   stp_clmn.type: clmn.my_type,
    #                   stp_clmn.uom_conversion_to_be_applied: clmn.uom_conversion_to_be_applied,
    #                   stp_clmn.nomenclature_to_be_applied: clmn.nomenclature_to_be_applied,
    #                   stp_clmn.string_cleaning_to_be_applied: clmn.string_cleaning_to_be_applied},
    #                  inplace=True)
    # df_column.set_index(clmn.clmn_var_name, inplace=True)

    print('pre loading 117')
    print(ps_stp.root_folder, ps_stp.json_folder)
    pre_load_objects_raw(df_column, root_json=ps_stp.root_folder, folder_json=ps_stp.json_folder)
    pre_load_mtx_objects(df_column, root_json=ps_stp.root_folder, folder_json=ps_stp.json_folder)
    return


if __name__ == "__main__":
    pre_load_objects()

import utils.setup
import variables.setup_column as stp_clmn
# import variables.DIAGEO_setup.file as stp
import variables.DIAGEO_setup.my_dict as stp_dct
import variables.dict as dct
import pandas as pd
import variables.general
from Dataset.Raw import RawDataset
from Dataset.Matrix import DataMatrix

pd.options.display.float_format = '{:.2f}'.format
pd.options.display.max_rows = 20000
pd.set_option('display.max_columns', 100)


def pre_load_objects_raw(df_column, any_stp_dict):
    # refactor -> both pre_load are almost the same
    df_setup = utils.setup.load_xlsx(filepath=any_stp_dict[dct.setup_filepath],
                                     sheet_name=any_stp_dict[dct.setup_raw_sheet_name],
                                     header=variables.general.header,
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
                                                      stp_clmn.clmn21, stp_clmn.clmn22,
                                                      stp_clmn.clmn23, stp_clmn.clmn24,
                                                      stp_clmn.clmn25,
                                                      stp_clmn.clmn26, stp_clmn.clmn27,
                                                      stp_clmn.clmn28, stp_clmn.clmn29,
                                                      stp_clmn.clmn30,
                                                      stp_clmn.to_transpose, stp_clmn.root,
                                                      stp_clmn.folder, stp_clmn.file,
                                                      stp_clmn.sheet, stp_clmn.format,
                                                      stp_clmn.usecols,
                                                      stp_clmn.nrows, stp_clmn.date_parser,
                                                      stp_clmn.skiprow_bf_header,
                                                      stp_clmn.skiprow_af_header,
                                                      stp_clmn.encoding, stp_clmn.decimal],
                                     key_clmn_list=[stp_clmn.dataset_name, stp_clmn.row],
                                     index_col=variables.general.index_col,
                                     usecols=variables.general.usecols,
                                     skiprows=variables.general.skiprows)

    any_raw_dataset_list = RawDataset.load_list_of_datasets(df_setup, df_column)

    for any_raw_dataset in any_raw_dataset_list:
        any_raw_dataset.write(any_stp_dict)

    return


def pre_load_mtx_objects(df_column, any_stp_dict):
    df_setup = utils.setup.load_xlsx(filepath=any_stp_dict[dct.setup_filepath],
                                     sheet_name=any_stp_dict[dct.setup_mtx_sheet_name],
                                     header=variables.general.header,
                                     new_column_list=[stp_clmn.row, stp_clmn.dataset_name,
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
                                                      stp_clmn.clmn21, stp_clmn.clmn22,
                                                      stp_clmn.clmn23, stp_clmn.clmn24,
                                                      stp_clmn.clmn25,
                                                      stp_clmn.clmn26, stp_clmn.clmn27,
                                                      stp_clmn.clmn28, stp_clmn.clmn29,
                                                      stp_clmn.clmn30,
                                                      stp_clmn.root, stp_clmn.folder],
                                     key_clmn_list=[stp_clmn.dataset_name, stp_clmn.row],
                                     index_col=variables.general.index_col,
                                     usecols=variables.general.usecols,
                                     skiprows=variables.general.skiprows)

    any_datamatrix_list = DataMatrix.load_list_of_datasets(df_setup, df_column)

    for any_datamatrix in any_datamatrix_list:
        any_datamatrix.write(any_stp_dict, save_dataframe=False, save_error=False)

    return


def pre_load_objects(stp_dict):

    df_column = utils.setup.load_xlsx(filepath=stp_dict[dct.setup_filepath],
                                      sheet_name=stp_dict[dct.column_sheet_name],
                                      header=variables.general.header,
                                      new_column_list=[stp_clmn.clmn_var_name,
                                                       stp_clmn.clmn_rep_name,
                                                       stp_clmn.type,
                                                       stp_clmn.uom_conversion_to_be_applied,
                                                       stp_clmn.nomenclature_to_be_applied,
                                                       stp_clmn.string_cleaning_to_be_applied],
                                      key_clmn_list=[stp_clmn.clmn_var_name],
                                      index_col=variables.general.index_col,
                                      usecols=variables.general.usecols,
                                      skiprows=variables.general.skiprows)

    pre_load_objects_raw(df_column, stp_dict)
    pre_load_mtx_objects(df_column, stp_dict)
    return


if __name__ == "__main__":
    pre_load_objects(stp_dct.setup_dict)



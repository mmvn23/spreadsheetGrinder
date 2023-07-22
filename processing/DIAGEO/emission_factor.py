import pandas as pd
from Dataset import Base, Raw, Matrix
import copy
import variables.dict as dct
import variables.DIAGEO_setup.my_dict as stp_dct
import variables.DIAGEO_setup.smartway_emission_factor as hard_code
import variables.var_column as clmn
import variables.datamatrix as mtx
import variables.general as var_gen
import utils.setup as ut_stp
import utils.general as ut_gen


def convert_string_to_dataframe(original_text, header_position=0, row_split_char='\n', column_split_char=' '):
    text = copy.deepcopy(original_text)

    row_list_before_split = text.split(row_split_char)
    row_list_after_split = []

    for any_row in row_list_before_split:
        any_row = any_row.split(column_split_char)
        row_list_after_split = row_list_after_split + [any_row]

    any_dataframe = pd.DataFrame()
    column_qty = len(row_list_after_split[header_position]) - 1
    row_qty = len(row_list_after_split)

    for column_index in range(column_qty, -1, -1):
        for row_index in range(1, row_qty):
            count_to_last = column_index - column_qty - 1
            any_dataframe.at[row_index - 1, row_list_after_split[header_position][column_index]] = \
                row_list_after_split[row_index][count_to_last]

            if column_index == 0:
                new_value = ''
                for new_count_to_last in range(0, len(row_list_after_split[row_index]) + count_to_last + 1):
                    new_value = new_value + row_list_after_split[row_index][new_count_to_last] + ' '
                new_value = new_value.strip()
                any_dataframe.at[row_index - 1, row_list_after_split[header_position][column_index]] = new_value

    return any_dataframe


def convert_text_from_page_to_df(any_text, tag):
    any_text = ut_gen.remove_list_of_expressions(original_list=hard_code.terms_to_remove,
                                                 text=any_text)
    any_text = ut_gen.remove_space_on_list_of_expressions(original_list=hard_code.terms_to_remove_space,
                                                          original_text=any_text)
    any_text = ut_gen.cut_string_based_on_tag(original_text=any_text, tag=tag)
    any_df = convert_string_to_dataframe(original_text=any_text, row_split_char='\n', column_split_char=' ')

    return any_df


def convert_text_from_multiple_pages_to_df(any_text_list, tag):

    any_df_emission = pd.DataFrame()

    for any_text in any_text_list:
        df_emission_to_append = convert_text_from_page_to_df(any_text=any_text, tag=tag)
        any_df_emission = pd.concat([any_df_emission, df_emission_to_append], ignore_index=True)

    return any_df_emission


def load_smartway_report(any_raw_name):
    any_raw_emission_factor_smartway = Raw.RawDataset.load_from_json(name=any_raw_name,
                                                                     root=stp_dct.setup_dict[dct.root_folder],
                                                                     folder=stp_dct.setup_dict[dct.json_folder])
    text_list = ut_stp.load_text_from_pdf(any_raw_emission_factor_smartway.filepath)
    df_emission = convert_text_from_multiple_pages_to_df(text_list, tag=hard_code.table_tag)

    any_raw_emission_factor_smartway.load_dataframe(load_from_spreadsheet=False, load_manually_from_dataframe=True,
                                                    dataframe_list=[df_emission])
    return any_raw_emission_factor_smartway


def load_smartway_report_list(any_raw_name_list):
    any_raw_emission_list = []

    for any_raw_name in any_raw_name_list:
        any_raw_emission_to_append = load_smartway_report(any_raw_name)
        any_raw_emission_list = any_raw_emission_list + [any_raw_emission_to_append]

    return any_raw_emission_list


def load_emission_factor(any_stp_dict):
    [mtx_nomenclature, mtx_uom_conversion, mtx_part_number] = Matrix.DataMatrix.load_old_object_list(
                                                                                ['nomenclature', 'uom_conversion',
                                                                                 'part_number'], any_stp_dict)

    any_mtx_emission_factor = Matrix.DataMatrix.load_from_json('emission_factor',
                                                                      root=stp_dct.setup_dict[dct.root_folder],
                                                                      folder=stp_dct.setup_dict[dct.json_folder])

    any_raw_emission_list = load_smartway_report_list(['raw_emission_factor_smartway_20',
                                                       'raw_emission_factor_smartway_21',
                                                       'raw_emission_factor_smartway_22',
                                                       'raw_emission_factor_smartway_23'])

    any_mtx_emission_factor.concat_base_dataset_list(any_basedataset_list=any_raw_emission_list)
    any_mtx_emission_factor.get_date_interval_from_fiscal_year(initial_date_clmn=clmn.initial_date,
                                                               end_date_clmn=clmn.end_date,
                                                               fiscal_year_clmn=clmn.fiscal_year)
    any_mtx_emission_factor.apply_nomenclature_to_column(any_column=clmn.si_uom, any_mtx_nomenclature=mtx_nomenclature)
    any_mtx_emission_factor.apply_nomenclature(any_mtx_nomenclature=mtx_nomenclature)
    any_mtx_emission_factor.apply_uom_conversion_to_si(any_mtx_uom_conversion=mtx_uom_conversion, any_mtx_part_number=var_gen.void,
                                    key_clmn=clmn.activity_code)
    any_mtx_emission_factor.apply_standard_index()
    any_mtx_emission_factor.add_timestamp()
    any_mtx_emission_factor.assure_column_integrity()
    any_mtx_emission_factor.remove_duplicated_index()

    any_mtx_emission_factor.write(any_stp_dict, save_dataframe=True, save_error=True)
    return


if __name__ == "__main__":
    load_emission_factor(any_stp_dict=stp_dct.setup_dict)


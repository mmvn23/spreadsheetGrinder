import copy
import variables.dict as dct
import pandas as pd
import utils.general as ut
import variables.type as tp
from Dataset import Raw, Matrix
import variables.var_column as clmn
import variables.DIAGEO_setup.my_dict as stp_dct


def load_mat_code_to_supplier_location(any_stp_dict):
    mtx_mat_code_to_supplier_location = Matrix.DataMatrix.load_from_json(name='mat_code_to_supplier_location',
                                                        root=any_stp_dict[dct.root_folder],
                                                        folder=any_stp_dict[dct.json_folder])
    [mtx_nomenclature] = Matrix.DataMatrix.load_old_object_list(['nomenclature'], any_stp_dict)
    mtx_mat_code_to_supplier_location.load_dataframe(any_raw_dataset_name_list=['mat_code_to_supplier_location_glass'],
                                                     any_mtx_nomenclature=mtx_nomenclature,
                                                     root_json=any_stp_dict[dct.root_folder],
                                                     folder_json=any_stp_dict[dct.json_folder])
    # mtx_mat_code_to_supplier_location.load_dataframe_from_family(base_dataset_family_name=
    #                                                              'mat_code_to_supplier_location_glass',
    #                                                              any_stp_dict=any_stp_dict,
    #                                                              treat_date=False, load_all_files_within_folder=False,
    #                                                              load_all_sheets_on_spreadsheet=True)

    return


if __name__ == "__main__":
    pd.set_option('display.max_columns', 10)
    pd.set_option('display.max_rows', 500)
    load_mat_code_to_supplier_location(stp_dct.setup_dict)
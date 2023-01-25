import copy
import variables.dict as dct
import pandas as pd
import utils.general as ut
import variables.type as tp
from Dataset import Raw, Matrix
import variables.var_column as clmn
import variables.DIAGEO_setup.my_dict as stp_dct


def load_part_number(any_stp_dict):
    mtx_part_number = Matrix.DataMatrix.load_from_json('part_number',
                                                       root=any_stp_dict[dct.root_folder],
                                                       folder=any_stp_dict[dct.json_folder])
    mtx_nomenclature = Matrix.DataMatrix.load_old_object('nomenclature', any_stp_dict)
    mtx_part_number.load_dataframe(any_raw_dataset_name_list=['part_number_glass'],
                                   any_mtx_nomenclature=mtx_nomenclature,
                                   root_json=any_stp_dict[dct.root_folder],
                                   folder_json=any_stp_dict[dct.json_folder])

    mtx_part_number.write(any_stp_dict, save_dataframe=True, save_error=True)

    return


if __name__ == "__main__":
    load_part_number(stp_dct.setup_dict)